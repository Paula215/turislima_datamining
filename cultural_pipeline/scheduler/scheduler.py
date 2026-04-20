#!/usr/bin/env python3
"""
scheduler.py
============
Configura la ejecución automática del pipeline cada domingo a las 2:00 AM.

Opciones de despliegue:
  A) schedule (biblioteca Python) — para servidores que corren 24/7
  B) crontab del sistema                  — para servidores Linux
  C) GitHub Actions (ver workflow.yml)    — para entornos cloud sin servidor

Uso:
  python scheduler.py              # Inicia el proceso daemon con schedule
  python scheduler.py --install-cron  # Instala entrada en crontab del sistema
  python scheduler.py --show-cron     # Solo muestra el comando cron
"""

import argparse
import subprocess
import sys
import logging
from pathlib import Path

ROOT = Path(__file__).parent.parent
PIPELINE_SCRIPT = ROOT / "pipeline" / "pipeline.py"
LOGS_DIR = ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scheduler] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "scheduler.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# A) schedule (Python daemon)
# ---------------------------------------------------------------------------

def run_pipeline():
    """Función que se ejecuta cada domingo a las 2 AM"""
    log.info("⏰ Ejecución programada iniciada")
    python = sys.executable
    result = subprocess.run(
        [python, str(PIPELINE_SCRIPT)],
        capture_output=False,
        text=True,
    )
    if result.returncode == 0:
        log.info("✅ Pipeline ejecutado exitosamente")
    else:
        log.error(f"❌ Pipeline falló con código {result.returncode}")


def start_daemon():
    """Inicia el proceso daemon usando la librería schedule"""
    try:
        import schedule
        import time
    except ImportError:
        log.error("Instala: pip install schedule")
        sys.exit(1)

    log.info("🤖 Scheduler daemon iniciado")
    log.info("📅 Próxima ejecución: cada domingo a las 02:00 AM (hora local del servidor)")

    schedule.every().sunday.at("02:00").do(run_pipeline)

    # También permite ejecución manual al arrancar si se desea
    log.info("Esperando... (Ctrl+C para detener)")
    while True:
        schedule.run_pending()
        import time as t
        t.sleep(60)


# ---------------------------------------------------------------------------
# B) Crontab del sistema
# ---------------------------------------------------------------------------

CRON_COMMENT = "# Cultural Pipeline Lima — pipeline.py"

def get_cron_line() -> str:
    python = sys.executable
    log_file = LOGS_DIR / "cron_run.log"
    return (
        f"0 2 * * 0 {python} {PIPELINE_SCRIPT} "
        f">> {log_file} 2>&1  {CRON_COMMENT}"
    )


def show_cron():
    print("\n📋 Agrega esta línea a tu crontab (ejecuta: crontab -e):")
    print("-" * 70)
    print(get_cron_line())
    print("-" * 70)
    print("Nota: cron usa la zona horaria local del servidor.")
    print("Significado del cron: 0 2 * * 0")
    print("  0      → minuto 0")
    print("  2      → hora 2 AM")
    print("  *      → cualquier día del mes")
    print("  *      → cualquier mes")
    print("  0      → domingo (0 = domingo en cron)\n")


def install_cron():
    """Instala automáticamente la entrada en el crontab del usuario"""
    cron_line = get_cron_line()

    # Leer crontab actual
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        current = result.stdout
    except Exception:
        current = ""

    if CRON_COMMENT in current:
        log.info("ℹ️  La entrada ya existe en el crontab")
        return

    new_crontab = current.rstrip() + "\n" + cron_line + "\n"

    proc = subprocess.run(["crontab", "-"], input=new_crontab, text=True, capture_output=True)
    if proc.returncode == 0:
        log.info("✅ Crontab actualizado exitosamente")
        show_cron()
    else:
        log.error(f"❌ Error al actualizar crontab: {proc.stderr}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scheduler para el pipeline cultural")
    parser.add_argument("--install-cron", action="store_true",
                        help="Instalar entrada en crontab del sistema")
    parser.add_argument("--show-cron", action="store_true",
                        help="Mostrar línea de crontab sin instalar")
    parser.add_argument("--run-now", action="store_true",
                        help="Ejecutar el pipeline ahora mismo")
    args = parser.parse_args()

    if args.show_cron:
        show_cron()
    elif args.install_cron:
        install_cron()
    elif args.run_now:
        run_pipeline()
    else:
        # Por defecto: iniciar daemon Python
        start_daemon()
