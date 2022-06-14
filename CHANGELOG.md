# Changelog

This Changelog tracks changes to this project. The notes below include a summary for each release, followed by details which contain one or more of the following tags:

- `added` for new features.
- `changed` for functionality and API changes.
- `deprecated` for soon-to-be removed features.
- `removed` for now removed features.
- `fixed` for any bug fixes.
- `security` in case of vulnerabilities.

## 14 Jun 2022

- `changed` bump API for schemas bump for shipping manifest requirement relaxing

## 9 Jun 2022

- `changed` bump API for schemas bump for added Microbiome support

## 8 Jun 2022

- `changed` bump API for schemas bump and counting for added ctDNA support

## 20 May 2022

- `changed` bump API for schemas bump for new ctDNA option for manifest assay_type

## 28 Apr 2022

- `changed` pytest, black, click, api version bumps

## 21 Apr 2022

- `changed` API (schemas) version bump for adding collection events to wes matching

## 6 Apr 2002

- `added` version pegs for flask-sqlalchemy error in CFn deploy

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
