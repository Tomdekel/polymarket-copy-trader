"""Background HTTP server for health checks."""
import json
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Callable, Optional

logger = logging.getLogger("polymarket_copy_trader")


class HealthStatus:
    """Tracks application health status."""

    def __init__(self):
        self.started_at: datetime = datetime.now()
        self.last_check: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.is_running: bool = True
        self.positions_count: int = 0
        self.trades_count: int = 0

    def update(
        self,
        positions_count: int = 0,
        trades_count: int = 0,
        error: Optional[str] = None,
    ) -> None:
        """Update health status."""
        self.last_check = datetime.now()
        self.positions_count = positions_count
        self.trades_count = trades_count
        if error:
            self.last_error = error

    def to_dict(self) -> dict:
        """Convert status to dictionary."""
        return {
            "status": "healthy" if self.is_running else "unhealthy",
            "started_at": self.started_at.isoformat(),
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "uptime_seconds": (datetime.now() - self.started_at).total_seconds(),
            "positions_count": self.positions_count,
            "trades_count": self.trades_count,
            "last_error": self.last_error,
        }


# Global health status instance
_health_status = HealthStatus()


def get_health_status() -> HealthStatus:
    """Get the global health status instance."""
    return _health_status


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health checks."""

    def log_message(self, format: str, *args) -> None:
        """Override to use our logger."""
        logger.debug("Health check: %s", format % args)

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/health" or self.path == "/":
            self._handle_health()
        elif self.path == "/ready":
            self._handle_ready()
        else:
            self._send_not_found()

    def _handle_health(self) -> None:
        """Handle health check endpoint."""
        status = get_health_status()
        response = status.to_dict()

        if status.is_running:
            self._send_json(200, response)
        else:
            self._send_json(503, response)

    def _handle_ready(self) -> None:
        """Handle readiness check endpoint."""
        status = get_health_status()

        # Ready if we've done at least one check
        if status.last_check is not None:
            self._send_json(200, {"ready": True})
        else:
            self._send_json(503, {"ready": False, "reason": "Not yet initialized"})

    def _send_json(self, status_code: int, data: dict) -> None:
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def _send_not_found(self) -> None:
        """Send 404 response."""
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"error": "Not found"}).encode())


class HealthServer:
    """Background HTTP server for health checks."""

    def __init__(self, port: int = 8080):
        self.port = port
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the health server in a background thread."""
        self._server = HTTPServer(("0.0.0.0", self.port), HealthHandler)
        self._server.socket.settimeout(10)  # 10 second timeout for requests
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info(f"Health server started on port {self.port}")

    def _run(self) -> None:
        """Run the server."""
        if self._server:
            self._server.serve_forever()

    def stop(self) -> None:
        """Stop the health server."""
        if self._server:
            self._server.shutdown()
            logger.info("Health server stopped")

        # Mark as not running
        get_health_status().is_running = False
