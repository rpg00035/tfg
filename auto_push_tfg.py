#!/usr/bin/env python3
"""
auto_push_tfg.py

Este script observa cambios reales en cualquier archivo de la carpeta TFG (excepto rutas o archivos ignorados)
y, cada vez que detecta una modificación/creación/eliminación/renombrado, hace automáticamente:
  1. GitPython: git add <rutas_cambiadas>
  2. GitPython: git commit -m "Auto-commit: <timestamp>"
  3. GitPython: git push origin <rama_actual>

Ventajas de esta versión:
- DEBOUNCE_DELAY más bajo (0.5 s) para agrupar menos tiempo.
- Sólo añade (stage) los ficheros que realmente cambiaron durante el debounce.
- Así GitPython no recorre todo el índice en cada cambio, ganando velocidad.
"""

import sys
import os
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from git import Repo, GitCommandError  # GitPython

# --------------------
# CONFIGURACIÓN
# --------------------

# Retraso (en segundos) para agrupar cambios seguidos.
DEBOUNCE_DELAY = 0.5  # medio segundo en lugar de 2 s

# Extensiones que queremos ignorar (archivos temporales, swap, etc.):
IGNORED_EXTS = {'.swp', '.swx', '.tmp', '.pyc', '.log', '.pkl', '.csv'}

# Directorios que NO deben vigilarse (p. ej. .git, venv, __pycache__, node_modules, etc.)
IGNORED_PATHS = {'.git', 'venv', '__pycache__', 'node_modules', 'enter'}

# Archivos concretos que NO deben vigilarse, aunque estén fuera de las carpetas ignoradas:
IGNORED_FILES = {'.gitignore'}

# Mensaje base de commit (se añade un timestamp automáticamente):
BASE_COMMIT_MSG = "Auto-commit TFG"

# --------------------
# LÓGICA DEL SCRIPT
# --------------------

class GitAutoPusher:
    def __init__(self, repo_path: str, branch: str = None):
        # Inicializamos el repositorio con GitPython
        try:
            self.repo = Repo(repo_path)
        except Exception as e:
            print(f"[❌ Error] No es un repositorio Git válido:\n  {e}")
            sys.exit(1)

        # Detectamos la rama actual si no se pasó explícitamente
        if branch:
            self.branch = branch
        else:
            self.branch = self.repo.active_branch.name

        # Ruta absoluta del repo (y forzamos cwd para comandos GitPython)
        self.repo_path = os.path.abspath(repo_path)
        os.chdir(self.repo_path)

        # Conjunto de rutas modificadas pendientes de añadir/commitear
        self.pending_paths = set()
        self._lock = threading.Lock()
        self._timer = None

        print(f"[👀 Watcher iniciado] Repositorio: {self.repo_path} | Rama: {self.branch}")

    def _run_git_commands(self):
        """
        Al disparar el debounce, este método:
        - Hace 'git add' sólo sobre las rutas que cambiaron.
        - Si hay cambios efectivamente añadidos, hace commit y push.
        - Vacía self.pending_paths.
        """
        with self._lock:
            paths_to_stage = list(self.pending_paths)
            self.pending_paths.clear()
            self._timer = None  # Reiniciamos timer

        if not paths_to_stage:
            # Raro, pero si no hay rutas en la lista, nada que hacer
            return

        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        commit_msg = f"{BASE_COMMIT_MSG} @ {ts}"

        try:
            # 1) 'git add <paths>'
            # GitPython acepta lista de rutas relativas al repo
            self.repo.index.add(paths_to_stage)

            # 2) 'git commit -m "mensaje"' (arroja GitCommandError si no hay nada nuevo)
            self.repo.index.commit(commit_msg)

            # 3) 'git push origin <branch>'
            origin = self.repo.remote(name='origin')
            origin.push(self.branch)

            print(f"[✅ Push ejecutado] {commit_msg}")
            print(f"    · Rutas incluidas: {paths_to_stage}")

        except GitCommandError as e:
            # Si no hay nada que commitear (p.ej., sólo eliminaste algo que ya estaba borrado),
            # GitCommandError suele contener "nothing to commit"
            msg = e.stderr.strip() if e.stderr else str(e)
            if "nothing to commit" in msg.lower():
                print("[ℹ️  Nada que commitear en este momento.]")
            else:
                print(f"[❌ Error al hacer push]:\n  {msg}")

    def schedule_push_for(self, path: str):
        """
        Agrega 'path' a pending_paths y reprograma el timer de debounce.
        """
        rel = os.path.relpath(path, self.repo_path)  # ruta relativa al repo
        with self._lock:
            self.pending_paths.add(rel)
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(DEBOUNCE_DELAY, self._run_git_commands)
            self._timer.start()

    def stop(self):
        """Detiene el timer si está activo."""
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None

class ChangeHandler(FileSystemEventHandler):
    def __init__(self, auto_pusher: GitAutoPusher):
        self.auto_pusher = auto_pusher

    def _should_ignore(self, path: str) -> bool:
        """
        Comprueba si la ruta (path) debe ignorarse:
        - Contiene algún directorio de IGNORED_PATHS en su ruta.
        - Coincide exactamente con un nombre de IGNORED_FILES.
        - Tiene extensión en IGNORED_EXTS.
        """
        # 1. ¿Está dentro de una carpeta a ignorar?
        for ign in IGNORED_PATHS:
            # Ejemplo: "/home/ruben/TFG/.git/config" → contiene "/.git/"
            if os.path.sep + ign + os.path.sep in path:
                return True
            # Si el cambio es exactamente el directorio (p.ej. renombrar .git)
            if path.endswith(os.path.sep + ign) or path.startswith(ign + os.path.sep):
                return True

        # 2. ¿Es un archivo a ignorar?
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

        # Sólo cuando cambia de verdad el contenido, no al abrir/cerrar sin modificar
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
        # Si un archivo se renombra o se mueve dentro del repo:
        if event.is_directory: return

        src, dst = event.src_path, event.dest_path
        # Si tanto origen como destino son ignorados, no hacemos nada.
        if self._should_ignore(src) and self._should_ignore(dst):
            return

        print(f"[🟠 Movido]   {src} --> {dst}")
        # Agregamos ambas rutas a pending_paths, para que Git sepa borrarlo y/o añadirlo:
        self.auto_pusher.schedule_push_for(src)
        self.auto_pusher.schedule_push_for(dst)

def main():
    # Obtenemos la ruta del repositorio (por defecto, cwd) y la rama (opcional)
    if len(sys.argv) > 2:
        print("Uso: python3 auto_push_tfg.py [<nombre_de_rama>]")
        sys.exit(1)

    branch_arg = sys.argv[1] if len(sys.argv) == 2 else None
    repo_path = os.getcwd()

    # Creamos el objeto que hará los pushes
    auto_pusher = GitAutoPusher(repo_path, branch_arg)

    # Configuramos el observer de watchdog
    observer = Observer()
    handler = ChangeHandler(auto_pusher)
    observer.schedule(handler, path=repo_path, recursive=True)

    try:
        observer.start()
        # Bucle infinito: interrumpe con Ctrl+C
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[✋ Detención solicitada. Parando watcher...]")
        observer.stop()
        auto_pusher.stop()
    observer.join() 

if __name__ == "__main__":
    main()
