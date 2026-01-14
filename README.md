## 🗡️ Experimental Image Preparation (2-static-verification-attack-scenarios)
This section describes how the experimental container images used for **static verification attack scenarios** are built and exported.

### 1️⃣ Move to the build directory
```bash
cd Dynamic_testing_container/ivi_container
```

### 2️⃣ Build the base image
The base image is used as a common parent for all attack scenario images.
```bash
podman build -t ivi_base:1.0 -f Containerfile .
```

### 3️⃣ Build attack scenario images
Each image is designed to trigger a specific static verification failure case.
```bash
podman build -t ivi_forbidden:2.3.1 -f Containerfile.forbidden .
podman build -t ivi_secret:2.3.2   -f Containerfile.secret .
podman build -t ivi_malicious:2.3.3 -f Containerfile.malicious .
podman build -t ivi_agpl:2.3.4     -f Containerfile.agpl .
podman build -t ivi_outdated:2.3.5 -f Containerfile.outdated .
```

### 4️⃣ Export images as OCI archives
Each built image is exported in **OCI archive format**, which is later consumed by the OTA static verification pipeline.
```bash
podman save --format oci-archive -o ivi_2.3.1.tar ivi_forbidden:2.3.1
podman save --format oci-archive -o ivi_2.3.2.tar ivi_secret:2.3.2
podman save --format oci-archive -o ivi_2.3.3.tar ivi_malicious:2.3.3
podman save --format oci-archive -o ivi_2.3.4.tar ivi_agpl:2.3.4
podman save --format oci-archive -o ivi_2.3.5.tar ivi_outdated:2.3.5
```
