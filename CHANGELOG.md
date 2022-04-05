# Changelog

This Changelog tracks changes to this project. The notes below include a summary for each release, followed by details which contain one or more of the following tags:

- `added` for new features.
- `changed` for functionality and API changes.
- `deprecated` for soon-to-be removed features.
- `removed` for now removed features.
- `fixed` for any bug fixes.
- `security` in case of vulnerabilities.

## 5 Apr 2022

- `changed` API (schemas) version bump for wes matching additions

## 25 Mar 2022

- `changed` API (schemas) version bump for regex version peg to prevent errors

## 1 Feb 2022

- `fixed` handling of subcases when applying permissions to multiple sets of files
- `added` flag for group email for BioFX group

## 31 Jan 2022

- `changed` API dependency for schemas bump for backward-compatible WES analysis

## 27 Jan 2022

- `removed` non-ACL based download permissions systems for production
- `changed` BioFX read access assigned during upload to use ACL
- `added` environment tag to email re disabling inactive users
- `added` this CHANGELOG
