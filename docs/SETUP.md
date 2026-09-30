# Verified development environment

The empty repository needed no previous checkout history. This task used Amazon Linux 2023, Python 3.12.14, PostgreSQL 16.14, Docker 25.0.16, and Compose 5.1.2. No user account credentials were required or committed. No Git commits or pushes were made.

## Native cloud sandbox

Installed Python through `mise install python@3.12`; created the repository `.venv`; installed pinned Python dependencies. Installed PostgreSQL using:

```bash
sudo dnf install -y postgresql16 postgresql16-server
```

A disposable, user-owned cluster lives outside the checkout at `/home/vercel-sandbox/runtime/postgres-eventvault/data`. It uses local Unix-socket trust for the sandbox OS user and SCRAM for TCP. It listens only on loopback. The local socket directory is `/home/vercel-sandbox/runtime/postgres-eventvault`.

```bash
pg_ctl -D /home/vercel-sandbox/runtime/postgres-eventvault/data \
  -l /home/vercel-sandbox/runtime/postgres-eventvault/server.log \
  -o '-k /home/vercel-sandbox/runtime/postgres-eventvault -p 5432 -h 127.0.0.1' start
pg_isready -h /home/vercel-sandbox/runtime/postgres-eventvault
export DATABASE_URL='postgresql:///eventvault?host=/home/vercel-sandbox/runtime/postgres-eventvault'
```

For a fresh sandbox, first run `initdb` on that directory with `--auth-local=trust --auth-host=scram-sha-256` and `createdb -h <socket-directory> eventvault`. These paths describe the verified task environment; use your normal managed PostgreSQL configuration elsewhere. The application start script is `/home/vercel-sandbox/runtime/start-eventvault.sh` outside Git.

## Docker networking

The sandbox initially had no running Docker daemon. Starting `dockerd` with task-local state succeeded with overlay2 and the default bridge network:

```bash
sudo dockerd \
  --data-root /home/vercel-sandbox/runtime/docker-eventvault/data \
  --exec-root /home/vercel-sandbox/runtime/docker-eventvault/exec \
  --pidfile /home/vercel-sandbox/runtime/docker-eventvault/docker.pid \
  --host unix:///home/vercel-sandbox/runtime/docker-eventvault/docker.sock
```

Run this daemon separately, redirect its log outside Git, and restrict socket ownership to the intended user/group (mode 660). Use `DOCKER_HOST=unix:///home/vercel-sandbox/runtime/docker-eventvault/docker.sock` for Docker commands. Do not change a working daemon on a normal development machine.

The sandbox's Buildx 0.12.1 was too old for Compose's build command. Building with `docker build -t eventvault:local .`, then `docker compose up -d --no-build`, worked. Compose service readiness and inventory creation were verified over the published API. The bridge network uses Docker DNS (`db`) and does not depend on a host LAN interface, device address, or host networking. The original request did not include a specific prior device/network configuration, so no such configuration was assumed.

Private Compose environment values are stored outside Git in `/home/vercel-sandbox/runtime/eventvault-compose.env`; the file is not a distributable artifact. The smoke-test stack used API port 8001 to avoid the native preview on 8000 and was stopped afterward while retaining its named database volume.
