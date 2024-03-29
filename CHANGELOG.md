# Changelog

This Changelog tracks changes to this project. The notes below include a summary for each release, followed by details which contain one or more of the following tags:

- `added` for new features.
- `changed` for functionality and API changes.
- `deprecated` for soon-to-be removed features.
- `removed` for now removed features.
- `fixed` for any bug fixes.
- `security` in case of vulnerabilities.

## 14 July 2023

- `added` transcriptome capture v6 to rna enrichment_method (see https://github.com/CIMAC-CIDC/cidc-api-gae/pull/815)

## 10 July 2023

- API bump for changing CIDC_MAILING_LIST to Essex-managed. See https://github.com/CIMAC-CIDC/cidc-api-gae/pull/813.

## 27 Apr 2023

- `changed` API/schemas bump for adding serum and allowing dna qc text

## 19 Apr 2023

- `changed` API/schemas bump for microbiome req reverse index

## 20 Mar 2023

- `fixed` bug causing error on cloud issuing of cross-trial permissions
  - this is interaction with accepted param types from API, so would need integrative testing

## 24 Feb 2023

- `changed` API bump for permissions bug fix
- `changed` better typing and variable handling in grant_permissions function

## 08 Feb 2023

- `removed` sending of dev alert email on permissions error

## 02 Feb 2023

- `changed` API bump for permission tweak

## 23 Jan 2023

- `changed` added trigger to file permissioning on manifest derived file creation

## 17 Jan 2023

- `changed` API bump for permission triggering for participants and samples info on upload
- `fixed` when ingesting derived manifest files mark upload type correctly

## 05 Jan 2023

- `changed` API/schemas bump to fix samples/participants prefix for file permissioning

## 03 Jan 2023

- `changed` API bump to not issue new permissions if user is disabled or not approved

## 28 Dec 2022

- `changed` API bump to
  - deduplicate blobs in revoke
  - fix blob prefix generation error on uploads without files
- `fixed` don't error if no users or blobs to apply

## 27 Dec 2022

- `changed` API/schemas bump to add urine to manifests' type of sample

## 20 Dec 2022

- `changed` API/schemas bump to remove ATACseq analysis batch report
- `changed` API/schemas bump to lowercase all buckets/instances for biofx
- `changed` API bump to fix bug in multifile upload_jobs setting

## 09 Dec 2022

- `changed` API/schemas bump for bug fix, new quality of sample option

## 02 Dec 2022

- `changed` API/schemas bump for updated permissions handling
- `changed` download permissions handling to accept list of upload_types

## 01 Dec 2022

- `changed` API/schemas bump for dateparser version update

## 30 Nov 2022

- `removed` permissioning of biofx groups in ingest_upload
  - separate from main permissions system in API
  - making concurrent updates to the same files via grant_download_permissions_for_upload_job
- `removed` unused is_group option in granting permissions
  - was only used by the above now-removed permissioning system
- `changed` API bump for parallel removal of is_group

## 28 Nov 2022

- `changed` API/schemas bump for WES analysis template folder update

## 17 Nov 2022

- `changed` API/schemas bump for wes bait set swap

## 10 Nov 2022

- `changed` API/schemas bump and handling for null return when there's no file derivation

## 08 Nov 2022

- `changed` API/schemas bump for adding batch to meta.csv for TCR

## 04 Nov 2022

- `changed` api/schemas bump: new front-page counting

## 03 Nov 2022

- `changed` API/schemas bump for adding meta.csv to TCR config returns, update local file path description

## 31 Oct 2022

- `changed` active user filter to check they are not disabled and approved
- `changed` api bump: on re-enable of unapproval user, do NOT apply BQ permissions

## 28 Oct 2022

- `changed` refresh just upload access for active users
  - as object lister IAM permissions and ACL-based download permissions don't expire

## 27 Oct 2022

- `changed` API/schemas bump for clinical count bug fix

## 24 Oct 2022

- `changed` API/schemas bump for MIBI updates

## 20 Oct 2022

- `changed` API version bump for consolidation of microbiome and ctdna analysis files

## 17 Oct 2022

- `changed` API version update and fix of function name
- `changed` API version bump for project reference fix

## 14 Oct 2022

- `changed` API bump for protobuf versioning and functionality fix

## 13 Oct 2022

- `changed` API bump for bigquery permission changes
- `added` granting bigquery permissions to download perm refresh

## 12 Oct 2022

- `changed` API bump for switching staging uploader role to temp replacement

## 10 Oct 2022

- `changed` API bump for MIBI support

## 06 Oct 2022

- `changed` API bump to fix bug in permissions for new uploads

## 01 Oct 2022

- `changed` API bump for new PACT User role

## 16 Sep 2022

- `changed` bump API for encrypting participant IDs

## 15 Sep 2022

- `changed` bump API for removal of relational tables
- `remove` function calls to removed functions in API

## 14 Sep 2022

- `changed` bump API for new docs / schemas tweaks

## 23 Aug 2022

- `fixed` bug putting wrong upload_type on derived files

## 17 Aug 2022

- `changed` bump API for microbiome metadata template changes and new shipping lab

## 15 Aug 2022

- `changed` bump API for ctdna analysis nulls, hande jpeg, and upload perm fix

## 11 Aug 2022

- `changed` bump API for clinical data NOT included in cross-assay permissions

## 9 Aug 2022

- `added` bump API for schemas bump for hande manifest req relaxation

## 2 Aug 2022

- `added` pinned flask version to fix error

## 2 Aug 2022

- `changed` bump API for schemas bump for hande req relaxation

## 27 Jul 2022

- `changed` bump API for schemas bump for clinical data participant counts fix

## 26 Jul 2022

- `changed` bump API for schemas bump for new participant alert on manifest upload

## 13 Jul 2022

- `changed` bump API for autogenerated WES analysis excel template tweak
- `changed` bump API again for schemas fix

## 12 Jul 2022

- `changed` bump API for schemas bump for autogenerated WES analysis excel templates

## 8 Jul 2022

- `changed` bump API for schemas bump for WES analysis pipeline v3

## 21 Jun 2022

- `changed` bump API for schemas bump for new wes bait set

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
