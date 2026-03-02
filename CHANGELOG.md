# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
