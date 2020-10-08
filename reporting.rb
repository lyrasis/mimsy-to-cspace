# frozen_string_literal: true

require_relative 'config'

## issues for migration
##  If anything comes up here, we'll need to do some more work
#Mimsy::AcquisitionSources.new_names
#Mimsy::ItemsMakers.new_names
#Mimsy::ItemsPeopleSources.new_names
#Mimsy::SourceDetails.new_names

## issues for client
#Mimsy::AcqItems.no_acquisition
#Mimsy::AcqItems.multi_acquisition
#Mimsy::AcqItems.multi_cat
#Mimsy::AcqItems.one_cat_report
#Mimsy::Cat.no_acq_items
#Mimsy::Cat.multi_acq_items
#Mimsy::Acquisition.no_acquisition_items
#Mimsy::AcqItems.duplicates
Mimsy::Location.objects_with_unmapped_home_locations
Mimsy::Location.objects_with_unmapped_regular_locations
#Mimsy::Measurements.duplicates
#Mimsy::Measurements.fractions
#Mimsy::Measurements.empty
#Mimsy::People.duplicates
#Cspace::Acquisition.duplicates
#Cspace::Media.no_filename_in_record
#Cspace::Media.duplicate_files
#Cspace::Media.duplicate_procedures
#Cspace::Media.duplicate_relationships
#Cspace::Media.orphan_media
#Cspace::Work.duplicates
#Cspace::CollectionObject.duplicates
#Cspace::Media.no_file_report
#Cspace::Media.in_s3_not_used

# informational
#Mimsy::AcqItems.one_acquisition
#Mimsy::AcqItems.no_cat
#Mimsy::AcqItems.one_cat
#Mimsy::Cat.one_acq_item
#Mimsy::Notepad.report
