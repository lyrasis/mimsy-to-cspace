# frozen_string_literal: true

# normalize_s3_file_list
#  - adds column with normalized filename to provided list of s3 bucket filenames
#  - flag duplicates on normalized form
# unique_files
#  - removes rows flagged as duplicates
# duplicate_files
#  - reports on duplicate files
# prep_data
#  - based on Mimsy items_media, and merging/using additional data from Mimsy catalogue,
#    mimsy media, and normalized AWS file list, produces table ready for reporting and
#    further processing into CollectionSpace-importable records
# no_file_report
#  - reports on media records with no associated file (record has filename, but file is not in S3 list)
# no_filename_in_record
#  - reports on media records with no filename
# csv
#  - export CSV of media handling records for import into CollectionSpace
# duplicate_procedures
#  - report on duplicate procedures not prepared for import
# relationship_to_object
#  - create CSV of object-media relationships for CollectionSpace import
# duplicate_relationships
#  - writes report of duplicate relationships that will not be included in migration
# orphan_media
#  - writes report of media handling/files not associated with any objects

module Cspace
  module Media
    extend self

    def normalize_s3_file_list
      awsjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/provided/s3_media_files.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform do |row|
          fn = row.fetch(:awsfilename, nil)
          fn ? row[:normawsfilename] = fn.downcase : row[:normawsfilename] = nil
          row
        end

        transform Deduplicate::Flag, on_field: :normawsfilename, in_field: :duplicate, using: @deduper
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/aws_norm_flagged.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nAWS FILENAMES NORMALIZED, DUPES FLAGGED"
          puts "#{@outrows} (of #{@srcrows}"
          puts "file: #{filename}"
        end
      end
      Kiba.run(awsjob)
    end

    def unique_files
      normalize_s3_file_list unless File.file?("#{DATADIR}/working/aws_norm_flagged.tsv")
      awsuniqjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/aws_norm_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/aws_norm.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nUNIQUE AWS NORMALIZED FILENAMES"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(awsuniqjob)
    end

    def duplicate_files
      normalize_s3_file_list unless File.file?("#{DATADIR}/working/aws_norm_flagged.tsv")
      awsdupesjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/aws_norm_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/DUPLICATE_norm_filenames.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nDUPLICATE AWS NORMALIZED FILENAMES"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(awsdupesjob)
    end

    def prep_data
      Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")
      unique_files unless File.file?("#{DATADIR}/working/aws_norm.tsv")
      
      mediajob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @procdeduper = {}
        @reldeduper = {}

        @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/catalogue.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :mkey)
        @med = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/media.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :mediakey)
        @aws = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/aws_norm.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :normawsfilename)

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/items_media.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }
        
        transform Delete::FieldsExcept, keepfields: %i[mkey mediakey media]

        transform Merge::MultiRowLookup,
          lookup: @cat,
          keycolumn: :mkey,
          fieldmap: {
            :object_id => :id_number
          }
        transform FilterRows::FieldPopulated, action: :keep, field: :object_id
        
        transform Merge::MultiRowLookup,
          lookup: @med,
          keycolumn: :mediakey,
          fieldmap: {
            :filename => :media_id
          }

        transform Merge::MultiRowLookup,
          lookup: @med,
          keycolumn: :mediakey,
          fieldmap: {
            rectype: :record_type,
            mediafilename: :media_id
          }
        transform Replace::FieldValueWithStaticMapping,
          source: :rectype,
          target: :type,
          mapping: MEDIATYPE,
          fallback_val: nil


        transform Merge::MultiRowLookup,
          lookup: @med,
          keycolumn: :mediakey,
          fieldmap: {
            repro_allowed: :repro_allowed
          }
        transform Replace::FieldValueWithStaticMapping,
          source: :repro_allowed,
          target: :repro,
          mapping: REPRO,
          fallback_val: nil
        
        transform Merge::MultiRowLookup,
          lookup: @med,
          keycolumn: :mediakey,
          fieldmap: {
            publishto: :publish
          }
        transform Replace::FieldValueWithStaticMapping,
          source: :publishto,
          target: :publishTo,
          mapping: PUBLISH,
          fallback_val: nil

        transform Merge::ConstantValue, target: :copyright, value: 'Copyright restrictions may apply. Permission to publish or reproduce must be secured from the repository and the copyright holder.'

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[copyright repro],
          target: :copyrightStatement,
          sep: ' '

        transform do |row|
          fn = row.fetch(:filename, nil)
          row[:normfilename] = fn ? row[:filename].downcase : nil
          row
        end
        
        transform Merge::MultiRowLookup,
          lookup: @aws,
          keycolumn: :normfilename,
          fieldmap: {
            awsfilename: :awsfilename,
            filesize: :filesize
          }

        transform do |row|
          filename = row.fetch(:awsfilename, nil)
          if filename
            row[:bloburi] = "https://breman-media.s3-us-west-2.amazonaws.com/#{filename}"
          else
            row[:bloburi] = nil
          end
          row
        end

        # # check if any media values are different from filename values
        # transform Delete::FieldValueIfEqualsOtherField,
        #   delete: :media,
        #   if_equal_to: :filename

        transform Delete::Fields, fields: %i[media]

        transform Deduplicate::Flag, on_field: :filename, in_field: :duplicate_procedure, using: @procdeduper

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[filename mkey],
          target: :medmkey,
          sep: ' ',
          delete_sources: false

        transform Deduplicate::Flag, on_field: :medmkey, in_field: :duplicate_relationship, using: @reldeduper
        #   show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/media_handling.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nMEDIA HANDLING DATA"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(mediajob)
    end

    def no_file_report
      prep_data unless File.file?("#{DATADIR}/working/media_handling.tsv")
      
      nofilereportjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :keep, field: :object_id
        transform FilterRows::FieldPopulated, action: :reject, field: :awsfilename
        transform Rename::Field, from: :filename, to: :identificationNumber
        #   show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/missing_media_files.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[identificationNumber],
          csv_options: CSVOPT
        post_process do
          puts "\n\nNO MATCHING FILE IN S3 BUCKET"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(nofilereportjob)
    end

    def no_filename_in_record
      prep_data unless File.file?("#{DATADIR}/working/media_handling.tsv")
      
      nomediaidreportjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :reject, field: :filename
        transform Rename::Field, from: :filename, to: :identificationNumber
        #   show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/missing_media_filenames.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[identificationNumber],
          csv_options: CSVOPT
        post_process do
          puts "\n\nNO FILENAME IN ITEMS_MEDIA"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(nomediaidreportjob)
    end

    def csv
      prep_data unless File.file?("#{DATADIR}/working/media_handling.tsv")
      
      mediaprocjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate_procedure, value: 'n'
        transform FilterRows::FieldPopulated, action: :keep, field: :filename
        transform FilterRows::FieldPopulated, action: :keep, field: :bloburi
        transform Delete::Fields, fields: %i[mkey mediakey object_id duplicate_procedure duplicate_relationship
                                             mediafilename normfilename awsfilename filesize medmkey]
        transform Rename::Field, from: :filename, to: :identificationNumber

        # SECTION BELOW recovers from not being able to batch delete media records via converter-tool 
        transform Merge::ConstantValue, target: :concat, value: 'media'
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[identificationNumber concat],
          target: :identificationNumber,
          sep: ' '
        # END SECTION
        
        #   show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/media_handling.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[identificationNumber],
          csv_options: CSVOPT
        post_process do
          puts "\n\nMEDIA HANDLING PROCEDURES"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(mediaprocjob)
    end

    def duplicate_procedures
      prep_data unless File.file?("#{DATADIR}/working/media_handling.tsv")
      
      mediadupejob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate_procedure, value: 'y'
        #   show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/DUPLICATE_media_handling.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nDUPLICATE FILENAMES OMITTED FROM MEDIA HANDLING PROCEDURES"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(mediadupejob)
    end

    def relationship_to_object
      prep_data unless File.file?("#{DATADIR}/working/media_handling.tsv")
      mediareljob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :keep, field: :object_id
        transform FilterRows::FieldPopulated, action: :keep, field: :filename
        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate_relationship, value: 'n'
        transform Delete::FieldsExcept, keepfields: %i[object_id filename]
        transform Merge::ConstantValue, target: :concat, value: 'media'
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[filename concat],
          target: :objectIdentifier,
          sep: ' '

        transform Merge::ConstantValue, target: :objectDocumentType, value: 'Media'
        transform Rename::Field, from: :object_id, to: :subjectIdentifier
        transform Merge::ConstantValue, target: :subjectDocumentType, value: 'CollectionObject'

        #   show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/rels_co-mh.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: CSVOPT
        post_process do
          puts "\n\nOBJECT-MEDIA HANDLING RELATIONSHIPS TO CREATE"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(mediareljob)
    end

    def duplicate_relationships
      prep_data unless File.file?("#{DATADIR}/working/media_handling.tsv")
      mediareldupejob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :keep, field: :object_id
        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate_relationship, value: 'y'

        #   show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/DUPLICATE_mediahandling-object_rels.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nDUPLICATE OBJECT-MEDIA HANDLING RELATIONSHIPS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(mediareldupejob)
    end

    def orphan_media
      prep_data unless File.file?("#{DATADIR}/working/media_handling.tsv")
      mediaomitjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/media_handling.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :reject, field: :object_id
        #   show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/media_handling_with_no_object_relationship.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: CSVOPT
        post_process do
          puts "\n\nMEDIA HANDLING WITH NO OBJECT"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(mediaomitjob)
    end
  end
end
