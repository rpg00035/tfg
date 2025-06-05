#!/usr/bin/env python3
"""
auto_push_tfg.py

Watcher que observa cambios en cualquier archivo de la carpeta TFG (ignorando rutas/archivos configurados),
y, cada vez que agrupa un lote de cambios, hace:

  1. GitPython: git add <rutas pendientes>
  2. GitPython: git commit -m "Auto-commit: <timestamp>"
  3. GitPython: git push origin <rama> --quiet

Para que sea más rápido, el 'push' se lanza en un hilo aparte y no bloquea la detección de nuevos cambios.

Uso:
  $ cd /ruta/a/TFG
  $ python3 auto_push_tfg.py [<rama>]

Si no indicas rama, se usa la rama actual del repositorio.
"""

import sys
import os
import time
import threading
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from git import Repo, GitCommandError

# --------------------
# CONFIGURACIÓN
# --------------------

# Retraso (en segundos) para agrupar cambios seguidos.
# 0.3 s suele ser suficiente para lotes pequeños y latencia reducida.
DEBOUNCE_DELAY = 0.3

# Extensiones que queremos ignorar (archivos temporales, swap, etc.)
IGNORED_EXTS = {'.swp', '.swx', '.tmp', '.pyc', '.log', '.pkl', '.csv'}

# Directorios que NO deben vigilarse (por ejemplo, .git, venv, __pycache__, node_modules, etc.)
IGNORED_PATHS = {'.git', 'venv', '__pycache__', 'node_modules', 'enter'}

# Archivos concretos que NO deben vigilarse, aunque estén fuera de las carpetas ignoradas
IGNORED_FILES = {'.gitignore'}

# Mensaje base de commit (se añade un timestamp automáticamente)
BASE_COMMIT_MSG = "Auto-commit TFG"

# --------------------
# LÓGICA DEL SCRIPT
# --------------------

class GitWorker(threading.Thread):
    """
    Hilo dedicado exclusivamente a hacer push al remoto sin bloquear el watcher.
    Recibe commits (objetos (paths_a_stage, mensaje)) a través de una cola (Queue) y los procesa.
    """
    def __init__(self, repo: Repo, branch: str, queue: Queue):
        super().__init__(daemon=True)
        self.repo = repo
        self.branch = branch
        self.queue = queue
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.is_set():
            try:
                # Esperamos hasta 1 segundo para recoger un trabajo; si no hay, seguimos en el bucle
                task = self.queue.get(timeout=1.0)
            except:
                continue  # no llegó nada en el timeout; loop de nuevo

            paths_to_stage, commit_msg = task

            try:
                # 1) git add <paths_to_stage>
                self.repo.index.add(paths_to_stage)

                # 2) git commit -m "mensaje"
                # Si no hay cambios, GitCommandError lanzará "nothing to commit"
                self.repo.index.commit(commit_msg)

                # 3) git push origin <branch> --quiet
                origin = self.repo.remote(name='origin')
                origin.push(self.branch, quiet=True)

                print(f"[✅ Push OK] {commit_msg}  · Archivos: {paths_to_stage}")

            except GitCommandError as e:
                stderr = e.stderr.strip() if e.stderr else str(e)
                if "nothing to commit" in stderr.lower():
                    # Nada que commitear: no es un error crítico
                    print("[ℹ️  Nada que commitear en este lote.]")
                else:
                    print(f"[❌ Error git]:\n  {stderr}")

            finally:
                self.queue.task_done()

    def stop(self):
        self._stop_event.set()


class GitAutoPusher:
    """
    Orquesta la preparación de lotes (paths pendientes) y envía tareas a la GitWorker.
    """
    def __init__(self, repo_path: str, branch: str = None):
        try:
            self.repo = Repo(repo_path)
        except Exception as e:
            print(f"[❌ Error] No es un repositorio Git válido:\n  {e}")
            sys.exit(1)

        # Detectar rama actual si no se recibe como argumento
        self.branch = branch or self.repo.active_branch.name

        # Ruta absoluta, cambia cwd para GitPython
        self.repo_path = os.path.abspath(repo_path)
        os.chdir(self.repo_path)

        # Conjunto de rutas pendientes de añadir
        self.pending_paths = set()
        self._lock = threading.Lock()

        # Cola para comunicar tareas (paths_a_stage, commit_msg) al worker
        self.queue = Queue()

        # Creamos y arrancamos el worker en background
        self.git_worker = GitWorker(self.repo, self.branch, self.queue)
        self.git_worker.start()

        # Timer para debounce
        self._timer = None

        print(f"[👀 Watcher iniciado] Repositorio: {self.repo_path} | Rama: {self.branch}")

    def _run_git_commands(self):
        """
        Al disparar el debounce, convertimos el conjunto de rutas pendientes en lista,
        formamos mensaje de commit y lo encolamos para que GitWorker lo procese.
        """
        with self._lock:
            paths_to_stage = list(self.pending_paths)
            self.pending_paths.clear()
            self._timer = None

        if not paths_to_stage:
            return

        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        commit_msg = f"{BASE_COMMIT_MSG} @ {ts}"
        # Encolamos directamente la tarea; el worker la recogerá y hará push asincrónico
        self.queue.put((paths_to_stage, commit_msg))

    def schedule_push_for(self, path: str):
        """
        Añade 'path' a pending_paths y reinicia el timer debounce.
        """
        rel = os.path.relpath(path, self.repo_path)
        with self._lock:
            self.pending_paths.add(rel)
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(DEBOUNCE_DELAY, self._run_git_commands)
            self._timer.start()

    def stop(self):
        """
        Detiene el timer y el worker.
        """
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
        self.git_worker.stop()
        self.git_worker.join()


class ChangeHandler(FileSystemEventHandler):
    """
    Manejador de eventos de watchdog que, ante creación/modificación/borrado/renombrado,
    decide si ignorar o encolar la ruta para commit/push.
    """
    def __init__(self, auto_pusher: GitAutoPusher):
        self.auto_pusher = auto_pusher

    def _should_ignore(self, path: str) -> bool:
        # 1. ¿Está dentro de alguna carpeta ignorada?
        for ign in IGNORED_PATHS:
            if os.path.sep + ign + os.path.sep in path:
                return True
            if path.endswith(os.path.sep + ign) or path.startswith(ign + os.path.sep):
                return True

        # 2. ¿Coincide con un archivo a ignorar?
        base = os.path.basename(path)
        if base in IGNORED_FILES:
            return True

        # 3. Extensiones a ignorar
        _, ext = os.path.splitext(path)
        if ext.lower() in IGNORED_EXTS:
            return True

        return False

    def on_modified(self, event):
        if event.is_directory: return
        if self._should_ignore(event.src_path): return

        print(f"[🟡 Modificado] {event.src_path}")
        self.auto_pusher.schedule_push_for(event.src_path)

    def on_created(self, event):
        if event.is_directory: return
        if self._should_ignore(event.src_path): return

        print(f"[🟢 Creado]   {event.src_path}")
        self.auto_pusher.schedule_push_for(event.src_path)

    def on_deleted(self, event):
        if event.is_directory: return
        if self._should_ignore(event.src_path): return

        print(f"[🔴 Borrado]  {event.src_path}")
        self.auto_pusher.schedule_push_for(event.src_path)

    def on_moved(self, event):
        if event.is_directory: return

        src, dst = event.src_path, event.dest_path
        # Si tanto origen como destino están ignorados, no encolamos nada
        if self._should_ignore(src) and self._should_ignore(dst):
            return

        print(f"[🟠 Movido]   {src} --> {dst}")
        self.auto_pusher.schedule_push_for(src)
        self.auto_pusher.schedule_push_for(dst)


def main():
    # Validación de argumentos (rama opcional)
    if len(sys.argv) > 2:
        print("Uso: python3 auto_push_tfg.py [<nombre_de_rama>]")
        sys.exit(1)

    branch_arg = sys.argv[1] if len(sys.argv) == 2 else None
    repo_path = os.getcwd()

    # Creamos el auto-pusher
    auto_pusher = GitAutoPusher(repo_path, branch_arg)

    # Montamos watchdog
    observer = Observer()
    handler = ChangeHandler(auto_pusher)
    observer.schedule(handler, path=repo_path, recursive=True)

    try:
        observer.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[✋ Detención solicitada. Parando watcher...]")
        observer.stop()
        auto_pusher.stop()
    observer.join() 


if __name__ == "__main__":
    main()
