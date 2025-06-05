#!/usr/bin/env python3
"""
auto_push_tfg.py

Este script observa cambios en cualquier archivo de la carpeta TFG (excepto rutas ignoradas)
y, cada vez que detecta una modificación, hace automáticamente:
  1. GitPython: git add .
  2. GitPython: git commit -m "Auto-commit: <timestamp>"
  3. GitPython: git push origin <rama_actual>

Puedes ejecutarlo desde la raíz de tu proyecto TFG:
  $ cd /ruta/a/TFG
  $ python3 auto_push_tfg.py [<rama>]

Si no indicas rama, detecta la rama actual automáticamente con GitPython.
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

# Retraso (en segundos) para agrupar varios cambios seguidos y no hacer 50 commits/minuto.
DEBOUNCE_DELAY = 2.0

# Extensiones que queremos ignorar (por ejemplo, archivos temporales, swap, etc.):
IGNORED_EXTS = {'.swp', '.swx', '.tmp', '.pyc', '.log', '.pkl', '.csv', '.gitignore'}

# Directorios que NO deben vigilarse (por ejemplo, .git, venv, __pycache__, node_modules, etc.)
IGNORED_PATHS = {'.git', 'venv', '__pycache__', 'node_modules', 'enter'} 

# Mensaje base de commit (se añadirá un timestamp automáticamente):
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
            print(f"[❌ Error] No parece ser un repositorio Git válido:\n  {e}")
            sys.exit(1)

        # Detectamos la rama actual si no se pasó explícitamente
        if branch:
            self.branch = branch
        else:
            self.branch = self.repo.active_branch.name

        # Guardamos la ruta absoluta del repo para cambiar cwd
        self.repo_path = os.path.abspath(repo_path)
        os.chdir(self.repo_path)

        self._lock = threading.Lock()
        self._timer = None

        print(f"[👀 Watcher iniciado] Repositorio: {self.repo_path} | Rama: {self.branch}")

    def _run_git_commands(self):
        """Ejecuta git add, commit y push usando GitPython."""
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        commit_msg = f"{BASE_COMMIT_MSG} @ {ts}"

        try:
            # 1) 'git add .'
            self.repo.git.add(all=True)

            # 2) 'git commit -m "mensaje"'
            # Si no hay cambios, GitCommandError se lanzará con un mensaje de "nothing to commit"
            self.repo.index.commit(commit_msg)

            # 3) 'git push origin <branch>'
            origin = self.repo.remote(name='origin')
            origin.push(self.branch)
            print(f"[✅ Push ejecutado] {commit_msg}")

        except GitCommandError as e:
            msg = e.stderr.strip() if e.stderr else str(e)
            # Si no hay nada para commitear, el mensaje suele contener "nothing to commit"
            if "nothing to commit" in msg.lower():
                print("[ℹ️  Nada que commitear en este momento.]")
            else:
                print(f"[❌ Error al hacer push]:\n  {msg}")

    def schedule_push(self):
        """Programa (o reprograma) el auto-push tras DEBOUNCE_DELAY segundos."""
        with self._lock:
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

    def on_any_event(self, event):
        # Si es un directorio, lo ignoramos
        if event.is_directory:
            return

        # Ignoramos rutas que contengan alguno de los directorios en IGNORED_PATHS
        for ign in IGNORED_PATHS:
            if os.path.sep + ign + os.path.sep in event.src_path:
                return
            if event.src_path.endswith(os.path.sep + ign) or event.src_path.startswith(ign + os.path.sep):
                return

        # Ignoramos extensiones no deseadas
        _, ext = os.path.splitext(event.src_path)
        if ext.lower() in IGNORED_EXTS:
            return

        # Si se crea/modifica/elimina/mueve un archivo relevante, disparar el auto-push
        print(f"[🟡 Detectado cambio] {event.event_type}: {event.src_path}")
        self.auto_pusher.schedule_push()

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
        # Bucle infinito: sólo interrumpe con Ctrl+C
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[✋ Detención solicitada. Parando watcher...]")
        observer.stop()
        auto_pusher.stop()
    observer.join()

if __name__ == "__main__":
    main()
