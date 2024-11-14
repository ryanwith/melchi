# Release Notes

## [1.0.1] - 2024-11-14
### Added
- Added Beta support for SSO authentication with Snowflake via browser

### Fixed
- Fixed bug in which started transactions continued to lock tables if you cancelled melchi operations via the keyboard

### Changed
- Some refactored code to make it easier to build new CDC types and integrations with different databases/data warehouses

## [1.0.0] - 2024-11-07
### Added
- Initial release
- Snowflake to DuckDB replication
- Support for full refresh, standard streams, and append-only streams
- Automatic schema creation and mapping
- Change tracking management
- Support for all Snowflake data types including GEOGRAPHY and GEOMETRY
- Transaction management and error handling
- Configuration via YAML, TOML, and CSV files

### Fixed
- Initial bug fixes

### Security
- Initial security features

## Format Guidelines

Release notes should be organized by:

1. Version Number and Date [x.y.z] - YYYY-MM-DD
2. Categories:
   - Added: New features
   - Changed: Changes to existing functionality
   - Deprecated: Soon-to-be removed features
   - Removed: Removed features
   - Fixed: Bug fixes
   - Security: Security updates

Link versions to their GitHub tags when using GitHub.