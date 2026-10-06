# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- SSH jumphost tunnel works again: `sshtunnel` is no longer used because it needs `paramiko.DSSKey`, which paramiko 4 and later removed. The local port forward is now implemented with paramiko directly

### Changed

- Bump `paramiko` from v4 to v5 (fixes CVE-2026-44405, SHA-1 RSA signatures)
- Drop the `sshtunnel` dependency
- Bump `actions/checkout` from v6 to v7
- Bump `actions/setup-python` from v6 to v7
- Bump `softprops/action-gh-release` from v2 to v3

## [1.2.0] - 2026-03-02

### Added

- PEP 723 inline script metadata for `uv run` support — run directly without cloning or manual dependency setup
- uv shebang (`#!/usr/bin/env -S uv run --script`) for direct execution with uv

## [1.1.0] - 2026-03-02

### Changed

- Bump `actions/checkout` from v4 to v6
- Bump `actions/setup-python` from v5 to v6
- Bump `paramiko` from v3 to v4
- Bump CI Python version from 3.12 to 3.14 in release workflow
- Pin `requests` dependency with minimum version (`>=2.32.0`) for Renovate tracking

## [1.0.0] - 2025-10-23

- Initial release
- Disk-based daily ingest estimation using per-index statistics
- SSH jumphost support via paramiko/sshtunnel
- Configurable time window (days, hours, minutes)
- Index pattern filtering with wildcards
- Per-index breakdown output
- Experimental ingest pipeline byte tracking mode
