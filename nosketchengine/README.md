# NoSketch Engine login

Both deployment workflows build `nosketchengine/Dockerfile`, push the corpus image
to GHCR, and deploy it through the shared Helm workflow. The browser asks for a
username and password before opening the UI. All UI and API paths require HTTP
Basic authentication; only the static `/healthz` endpoint is public.

## Set up credentials

1. Generate a bcrypt entry locally with Apache's `htpasswd`:

   ```bash
   htpasswd -nB -C 12 corpus-user
   ```

   Enter the password at the prompt. The output is `username:bcrypt-hash`.
   Do not use `-b`, which puts the plaintext password in command arguments.
2. Add the output as a **repository Actions secret** named `NOSKE_HTPASSWD` in
   GitHub Settings → Secrets and variables → Actions. For multiple users, include
   one generated entry per line. Usernames may contain letters, digits, `_`, `.`,
   `@`, and `-`. Bcrypt costs from 10 through 16 are accepted.
3. Run either deployment workflow. Missing, empty, or invalid credentials fail
   the build. The build job does not select the `production` environment, so an
   environment-only secret will not be available to it.

The hash is passed using a required BuildKit secret mount, never a build argument
or environment variable. The image contains the username and bcrypt verifier in
`/etc/lighttpd/noske.htpasswd`, outside the web root, readable by root and the
server group. The plaintext password never enters CI or the image. Anyone able
to pull the image can extract the verifier and attempt offline password guessing:
use a strong unique password and keep the GHCR package private. Build-time
authentication cannot keep the verifier secret from image readers.
For a private package, the cluster also needs GHCR pull credentials through its
service account or a pre-created secret listed in `image.secrets` in
`.github/auto-deploy-values.yaml`; the CI registry login only authorizes the build.

Serve the public URL over HTTPS with a valid ingress certificate. Basic auth sends
credentials with each request; TLS terminates at the ingress. The Helm values
enable HTTP-to-HTTPS redirection and send Kubernetes probes to `/healthz` without
embedding credentials in the deployment configuration. Verify HTTPS on every
configured ingress hostname before sharing credentials.

## Local build and credential rotation

Keep a local htpasswd file outside the repository and build with:

```bash
docker build -f nosketchengine/Dockerfile \
  --secret id=noske_htpasswd,src=/absolute/path/outside/repository/noske.htpasswd \
  --no-cache-filter authenticated \
  -t dioedb-noske .
```

After changing `NOSKE_HTPASSWD`, rerun a deployment workflow. Both workflows bypass
the `authenticated` stage cache so a secret change takes effect while preserving
the corpus compilation cache. Existing images retain their previous verifier;
rotate by deploying the rebuilt image.

Each workflow run and rerun uses a unique `run-ID-ATTEMPT-SHA` deployment tag, so
rotating credentials also changes the Kubernetes pod template and starts new pods.
The shared deployment workflow adds the short SHA to the repository name passed
as `DOCKER_TAG`; callers must not pass an already tagged image name.

The Dockerfile-specific ignore file limits the build context to corpus verticals
and the required NoSketch Engine configuration. Local htpasswd files are also
ignored by Git; keep them outside the repository as described above.

The root-level `Dockerfile` is a separate local build recipe; use the authenticated
recipe above for deployments.
