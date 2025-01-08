# Database Schema: your_database_name

## Table: baseline_characteristics
- groupId
- nct_id
- title
- status
- numSubjects
- Female_count
- Male_count
- numSubjects_who_completed
- studyStatus

## Table: clinicaltrial
- nct_id
- is_Rare
- lastUpdateSubmitDate
- org_name
- org_study_class
- responsibleParty
- primaryCompletionDate
- overallStatus
- hasExpandedAccess
- startDate
- completionDate
- lead_sponsor
- sponsor_class
- oversighthasdmc
- isfdaregulateddrug
- conditions
- keywords
- study_type
- phases
- interventionModel
- primaryPurpose
- mask_type
- theorical_enrollment_count
- last_enrollment_count
- arm_group_count
- intervention_type
- timePerspective
- healthyVolunteers
- sex
- minimumAge
- maximumAge
- AgeMetric
- stdAges
- has_results
- why_stopped
- randomized
- nb_secondaryOutcomes
- nb_primaryOutcomes
- total_centers
- number_of_collaborators
- country
- continent

## Table: clinicaltrial_extra
- nct_id
- is_Rare
- overallStatus
- startYear
- last_enrollment_count
- original_enrollment_count
- enrolled_less_than_original
- enrollmentCountSame
- conditions_count
- masking_count
- Participant_masked
- Care_provider_masked
- Investigator_masked
- Outcomes_assessor_masked
- condition_mesh_count
- intervention_mesh_count
- primaryoutcome_count
- secondaryoutcome_count
- original_primaryoutcome_count
- original_secondaryoutcome_count
- topic_drugInfo
- topic_rareDiseases
- topic_medlinePlus
- topic_medlinePlusGenetics
- primaryOutcomesSame
- secondaryOutcomesSame
- orgFullNameSame
- responsiblePartySame
- leadSponsorSame
- versions_count
- experimental_only
- exp_against_placebo
- exp_against_active_comp

## Table: collaborators
- name
- class
- nct_id
- number_of_collaborators

## Table: gdp_details
- country
- continent
- GDP_2022
- Corruption_indice_2022

## Table: locations
- facility
- startDate
- city
- status
- state
- country
- continent
- lat
- lon
- nct_id
- total_center
- location_number

## Table: pmid_references
- nct_id
- pmid
- type
- citation

## Table: responsibleparty_location
- nct_id
- responsible_party
- startDate
- lead_sponsor
- sponsor_class
- PI_full_name
- PI_title
- PI_affiliation
- city
- country
- continent
- lat
- lon

## Table: secondary_ids
- nct_id
- sec_id
- type
- domain

## Table: study_exp_drugs
- nct_id
- Intervention_type
- name
- Drug_type
- other_names
- potential_match
- experimental_only
- exp_against_placebo
- exp_against_active_comp

## Table: termes_mesh
- term
- mesh_id
- mesh_id_formatted
- tree_level
- mesh_category
- nct_id

## Table: topic_druginfo
- nct_id
- drugInfo
- drugbank_id

## Table: withdraws_type
- groupId
- nct_id
- numSubjects
- dropWithdraws_type
- overallStatus
- why_stopped

