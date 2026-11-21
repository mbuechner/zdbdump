"""Compare a release with the exact base manifests used for its build platform."""

import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys


BUILD_INPUTS_LABEL = "de.ddb.labs.zdbdump.build-inputs"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
IMAGE = re.compile(
    r"[a-z0-9][a-z0-9._-]*(?::[0-9]+)?(?:/[a-z0-9][a-z0-9._-]*)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?(?:@sha256:[0-9a-f]{64})?"
)
TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")


def dockerfile_lines(dockerfile):
    """Join continued Dockerfile instructions while ignoring standalone comments."""
    continuation = ""
    for physical in dockerfile.read_text(encoding="utf-8-sig").splitlines():
        line = physical.strip()
        if not line or line.startswith("#"):
            continue
        if line.endswith("\\"):
            continuation += line[:-1]
            continue
        yield continuation + line
        continuation = ""
    if continuation:
        raise ValueError(f"Incomplete instruction in {dockerfile}.")


def base_images(dockerfile, platform):
    """Read literal FROM images and resolve the base of the final stage."""
    stages = {}
    bases = []
    runtime = ""
    for line in dockerfile_lines(dockerfile):
        instruction, *rest = line.split(None, 1)
        if instruction.upper() != "FROM":
            continue

        fields = shlex.split(rest[0], comments=True) if rest else []
        if fields and fields[0].startswith("--platform="):
            specified = fields.pop(0).partition("=")[2]
            if specified not in (platform, "$TARGETPLATFORM", "${TARGETPLATFORM}"):
                raise ValueError(f"Unsupported FROM platform in {dockerfile}: {specified}.")
        if not fields or (len(fields) != 1 and (len(fields) != 3 or fields[1].upper() != "AS")):
            raise ValueError(f"Unsupported FROM instruction in {dockerfile}: {line}.")

        image = fields[0]
        if image.lower() in stages:
            runtime = stages[image.lower()]
        elif image.lower() == "scratch":
            runtime = ""
        else:
            if not IMAGE.fullmatch(image):
                raise ValueError(f"FROM must use a literal image in {dockerfile}: {image}.")
            runtime = image
            bases.append(image)
        if len(fields) == 3:
            stages[fields[2].lower()] = runtime

    if not bases:
        raise ValueError(f"No external base images found in {dockerfile}.")
    return list(dict.fromkeys(bases)), runtime


def inspect(image, template, *, allow_missing=False):
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", "--format", template, image],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=90,
    )
    if result.returncode:
        error = result.stderr.strip()
        reason = error.lower()
        missing = "manifest unknown" in reason or "name unknown" in reason or f"{image.lower()}: not found" in reason
        if allow_missing and missing:
            return None
        raise RuntimeError(error or f"Could not inspect {image}.")
    return json.loads(result.stdout)


def descriptor_digest(descriptor, image):
    digest = descriptor.get("digest", "")
    if not DIGEST.fullmatch(digest):
        raise ValueError(f"Registry returned an invalid digest for {image}.")
    return digest


def image_ref(image, digest):
    return f"{image.partition('@')[0]}@{digest}"


def matches_platform(details, platform):
    os_name, architecture, *variant = platform.split("/")
    return (details.get("os") == os_name and details.get("architecture") == architecture
            and (not variant or details.get("variant") == variant[0]))


def platform_digest(image, manifest, platform):
    if "manifests" in manifest:
        matches = [entry for entry in manifest["manifests"] if matches_platform(entry.get("platform", {}), platform)]
        if len(matches) == 1:
            return descriptor_digest(matches[0], image)
    else:
        digest = descriptor_digest(manifest, image)
        config = inspect(image_ref(image, digest), "{{json .Image}}")
        if matches_platform(config, platform):
            return digest
    raise ValueError(f"Expected one {platform} manifest for {image}.")


def reusable_digest(image, platform, fingerprint):
    manifest = inspect(image, "{{json .Manifest}}", allow_missing=True)
    if manifest is None:
        return ""
    digest = descriptor_digest(manifest, image)
    config_digest = digest
    if "manifests" in manifest:
        # A manifest list may also contain BuildKit attestations. Reuse only when there is one runnable target.
        runnable = [entry for entry in manifest["manifests"] if
                    entry.get("annotations", {}).get("vnd.docker.reference.type") != "attestation-manifest"]
        if len(runnable) != 1 or not matches_platform(runnable[0].get("platform", {}), platform):
            return ""
        config_digest = descriptor_digest(runnable[0], image)
    config = inspect(image_ref(image, config_digest), "{{json .Image}}")
    labels = (config.get("config") or {}).get("Labels") or {}
    if matches_platform(config, platform) and labels.get(BUILD_INPUTS_LABEL) == fingerprint:
        return digest
    return ""


def main():
    platform = os.environ.get("PLATFORM", "linux/amd64")
    dockerfile = Path(os.environ.get("DOCKERFILE", "release-source/Dockerfile"))
    image = os.environ["IMAGE"]
    tag = os.environ["RELEASE_TAG"]
    revision = os.environ["RELEASE_REVISION"]
    if not re.fullmatch(r"[a-z0-9_]+/[a-z0-9_]+(?:/[a-z0-9_]+)?", platform):
        raise ValueError(f"Invalid build platform: {platform}.")
    if not IMAGE.fullmatch(image) or not TAG.fullmatch(tag) or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", revision):
        raise ValueError("Invalid release image, tag, or commit SHA.")

    bases, runtime = base_images(dockerfile, platform)
    digests = {}
    for base in bases:
        manifest = inspect(base, "{{json .Manifest}}")
        digests[base] = platform_digest(base, manifest, platform)
    inputs = {"revision": revision, "platform": platform, "bases": digests}
    fingerprint = hashlib.sha256(json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    release_image = f"{image}:{tag}"
    digest = reusable_digest(release_image, platform, fingerprint)

    # Pin literal FROM references without rewriting the release Dockerfile.
    contexts = {base: image_ref(base, digests[base]) for base in bases if "@" not in base}
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        for name, value in (
            ("base_name", runtime), ("base_digest", digests.get(runtime, "")),
            ("fingerprint", fingerprint), ("reusable_digest", digest),
        ):
            print(f"{name}={value}", file=output)
        print("contexts<<BASE_IMAGES", file=output)
        for base, reference in contexts.items():
            print(f"{base}=docker-image://{reference}", file=output)
        print("BASE_IMAGES", file=output)
    print(f"Reusing {release_image}@{digest}." if digest else f"Building {release_image} from resolved inputs.")
    for base, resolved in digests.items():
        print(f"  {base} -> {resolved}")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        sys.exit(str(error))
