# frozen_string_literal: true

# all_acq
#  - prepare all rows from mimsy acquisitions table for CollectionSpace import
#  - flags duplicates
# csv
#  - outputs non-duplicates for CollectionSpace import
# duplicates
#  - writes report of duplicates that will not be imported
# relationship_to_object
#  - creates importable CSV of catalogued objects-to-acquisition relatiohships
# relationship_to_acqitem_object
#  - creates importable CSV of acquisition_item-sourced object-to-acquisition relationships
module Cspace
  module Acquisition
    extend self

    def all_acq
      Mimsy::Acquisition.working unless File.file?("#{DATADIR}/working/acquisitions.tsv")
      Mimsy::Acquisition.add_sources unless File.file?("#{DATADIR}/working/acquisition_sources.tsv")
      
      acqjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0


        @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :akey)
        @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acquisitions.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :akey)
        @src = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acquisition_sources.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :akey)

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisitions.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # ref number is required
        transform FilterRows::FieldPopulated, action: :keep, field: :ref_number
        # get rid of acq fields we are going to merge in from copy
        transform Delete::Fields, fields: %i[status status_date requested_by request_date legal_date legal_date_display]

        transform Merge::MultiRowLookup,
          lookup: @src,
          keycolumn: :akey,
          fieldmap: {
            :acquisitionSourcePerson => :preferred_name,
          },
          conditions: {
            include: {
              :field_equal => { fieldsets: [
                {matches: [
                  ['mergerow::individual','value::Y']
                ]}
              ]}
            }
          },
          delim: MVDELIM  

        transform Merge::MultiRowLookup,
          lookup: @src,
          keycolumn: :akey,
          fieldmap: {
            :acquisitionSourceOrganization => :preferred_name,
          },
          conditions: {
            include: {
              :field_equal => { fieldsets: [
                {matches: [
                  ['mergerow::individual','value::N']
                ]}
              ]}
            }
          },
          delim: MVDELIM  
        
        transform Merge::MultiRowLookup,
          lookup: @acq,
          keycolumn: :akey,
          fieldmap: {
            :approvalStatus => :status,
            :approvalDate => :status_date
          },
          constantmap: {
            :approvalIndividual => ''
          },
          delim: MVDELIM

        transform Clean::DowncaseFieldValues,
          fields: %i[approvalStatus]
        
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[approvalStatus],
          find: '^acknowledges?$',
          replace: 'acknowledged'
        
        transform Merge::MultiRowLookup,
          lookup: @acq,
          keycolumn: :akey,
          fieldmap: {
            :approvalIndividualReq => :requested_by,
            :approvalDateReq => :request_date
          },
          constantmap: {
            :approvalStatusReq => 'requested'
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @acq,
          keycolumn: :akey,
          fieldmap: {
            :approvalDateLegal => :legal_date
          },
          constantmap: {
            :approvalStatusLegal => 'legal',
            :approvalIndividualLegal => ''
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @acq,
          keycolumn: :akey,
          fieldmap: {
            :approvalDateLegalDisp => :legal_date_display
          },
          constantmap: {
            :approvalStatusLegalDisp => 'legal',
            :approvalIndividualLegalDisp => ''
          },
          delim: MVDELIM


        transform Merge::MultiRowLookup,
          lookup: @acqitems,
          keycolumn: :akey,
          fieldmap: {
            :aiApprovalStatus => :status,
            :aiApprovalDate => :status_date
          },
          constantmap: {
            :aiApprovalIndiv => ''
          },
          delim: MVDELIM

        transform Clean::DowncaseFieldValues,
          fields: %i[aiApprovalStatus]
        
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[aiApprovalStatus],
          find: '^\\\\$',
          replace: ''
        
        transform CombineValues::AcrossFieldGroup,
          fieldmap: {
            :approvalStatus => %i[approvalStatusReq approvalStatus aiApprovalStatus approvalStatusLegal approvalStatusLegalDisp],
            :approvalIndividual => %i[approvalIndividualReq approvalIndividual aiApprovalIndiv approvalIndividualLegal approvalIndividualLegalDisp],
            :approvalDate => %i[approvalDateReq approvalDate aiApprovalDate approvalDateLegal approvalDateLegalDisp]
          },
          sep: MVDELIM

        transform Clean::EmptyFieldGroups,
          groups: [
            %i[approvalStatus approvalIndividual approvalDate]
          ],
          sep: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @acq,
          keycolumn: :akey,
          fieldmap: {
            :groupPurchasePriceValue => :total_offer_price
          },
          constantmap: {
            :groupPurchasePriceCurrency => 'US Dollar'
          },
          delim: MVDELIM

        transform Merge::MultiRowLookup,
          lookup: @acqitems,
          keycolumn: :akey,
          fieldmap: {
            :aiAcquisitionNote => :note,
            :accessionDateGroup => :accession_date
          },
          delim: MVDELIM

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[decision_reason aiAcquisitionNote],
          target: :acquisitionNote,
          sep: MVDELIM

        # SECTION below standardizes :method values
        transform Delete::FieldValueMatchingRegexp,
          fields: [:method],
          match: '^unkown$'

        transform Clean::RegexpFindReplaceFieldVals,
          fields: [:method],
          find: ' - no ack\. necessary',
          replace: ''

        transform Clean::RegexpFindReplaceFieldVals,
          fields: [:method],
          find: ' ',
          replace: ''

        transform Clean::DowncaseFieldValues, fields: %i[method]
        # END SECTION
        
        transform Rename::Field, from: :authorized_by, to: :acquisitionAuthorizer
        transform Rename::Field, from: :date_authorized, to: :acquisitionAuthorizerDate
        transform Rename::Field, from: :method, to: :acquisitionMethod
        transform Rename::Field, from: :reason, to: :acquisitionReason
        transform Rename::Field, from: :ref_number, to: :acquisitionReferenceNumber
        transform Rename::Field, from: :terms, to: :acquisitionProvisos
        transform Rename::Field, from: :total_offer_price, to: :groupPurchasePriceValue

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[acquisitionProvisos],
          find: 'LINEBREAKWASHERE',
          replace: "\n"


        transform Merge::ConstantValueConditional,
          fieldmap: {acquisitionMethod: 'gift'},
          conditions: {
            include: {
              field_empty: {
                fieldsets: [
                  {fields: %w[row::acquisitionMethod]}
                ]
              },
              field_equal: {
                fieldsets: [
                  {matches: [
                    %w[row::acquisitionReason revalue::[Gg]ift]
                  ]}
                ]
              }
            }
          }

        transform Merge::ConstantValueConditional,
          fieldmap: {acquisitionMethod: 'transfer'},
          conditions: {
            include: {
              field_empty: {
                fieldsets: [
                  {fields: %w[row::acquisitionMethod]}
                ]
              },
              field_equal: {
                fieldsets: [
                  {matches: [
                    %w[row::acquisitionReason revalue::[Tt]ransfer]
                  ]}
                ]
              }
            }
          }

        # remove "gift" and "transfer" from acquisitionReason when they have been conditionally
        #  entered in acquisitionMethod
        transform Deduplicate::Fields, source: :acquisitionMethod, targets: %i[acquisitionReason],
          casesensitive: false
        
        transform Deduplicate::Flag, on_field: :acquisitionReferenceNumber, in_field: :duplicate, using: @deduper

        transform Delete::Fields, fields: %i[akey status requested_by request_date legal_date total_requested
                                             external_file aiTransferDate]

        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = 'data/working/acquisitions_duplicates_flagged.tsv'
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[acquisitionReferenceNumber],
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nACQ RECORDS WITH DUPLICATES FLAGGED"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(acqjob)
    end

    def csv
      all_acq unless File.file?("#{DATADIR}/working/acquisitions_duplicates_flagged.tsv")
      
      csacq = Kiba.parse do
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acquisitions_duplicates_flagged.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'

        transform Delete::Fields, fields: %i[duplicate]

        transform{ |r| @outrows += 1; r }
        filename = 'data/cs/acquisitions.csv'
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: CSVOPT
        post_process do
          puts "\n\nCSPACE ACQ RECORDS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(csacq)
    end
    
    def duplicates
      all_acq unless File.file?("#{DATADIR}/working/acquisitions_duplicates_flagged.tsv")

      dupeacq = Kiba.parse do
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acquisitions_duplicates_flagged.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'

        transform Delete::Fields, fields: %i[duplicate]

        transform{ |r| @outrows += 1; r }
        filename = 'data/working/acquisitions_duplicates.tsv'
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nDUPLICATE ACQ RECORDS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(dupeacq)
    end

    def relationship_to_object
      Mimsy::AcqItems.id_lookup unless File.file?("#{DATADIR}/working/acqitem_link.tsv")
      Mimsy::Acquisition.id_lookup unless File.file?("#{DATADIR}/working/acq_link.tsv")
      Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")

      co_acq_rel = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        @aikeys = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acqitem_link.tsv",
                                           csvopt: TSVOPT,
                                           keycolumn: :m_id)
        @acqkeys = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acq_link.tsv",
                                            csvopt: TSVOPT,
                                            keycolumn: :akey)

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }
        
        transform Delete::FieldsExcept, keepfields: %i[mkey id_number]

        transform Merge::MultiRowLookup,
          lookup: @aikeys,
          keycolumn: :mkey,
          fieldmap: {
            akey: :akey,
          },
          delim: MVDELIM

        #show_me!
        transform FilterRows::FieldPopulated, action: :keep, field: :akey

        transform Explode::RowsFromMultivalField, field: :akey, delim: ';'

        transform Merge::MultiRowLookup,
          lookup: @acqkeys,
          keycolumn: :akey,
          fieldmap: {
            objectIdentifier: :ref_number
          },
          constantmap: {
            objectDocumentType: 'Acquisition'
          },
          delim: MVDELIM

        transform Rename::Field, from: :id_number, to: :subjectIdentifier
        transform Merge::ConstantValue, target: :subjectDocumentType, value: 'CollectionObject'

        transform Delete::Fields, fields: %i[mkey akey]

        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/rel_acq-co.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: CSVOPT
        
        post_process do
          puts "\n\nCOLLECTIONOBJECT-TO-ACQUISITION RELATIONSHIPS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end  
      end
      Kiba.run(co_acq_rel)
    end
    
    def relationship_to_acqitem_object
      Mimsy::AcqItems.id_lookup unless File.file?("#{DATADIR}/working/acqitem_link.tsv")
      Mimsy::Acquisition.id_lookup unless File.file?("#{DATADIR}/working/acq_link.tsv")
      
      acqitem_acq_rel = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        @acqkeys = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acq_link.tsv",
                                            csvopt: TSVOPT,
                                            keycolumn: :akey)
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_link.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # Only process rows we used to create a stub (not cataloged) collectionobject
        transform FilterRows::FieldPopulated, action: :reject, field: :m_id

        transform Merge::MultiRowLookup,
          lookup: @acqkeys,
          keycolumn: :akey,
          fieldmap: {
            objectIdentifier: :ref_number
          },
          constantmap: {
            objectDocumentType: 'Acquisition'
          },
          delim: MVDELIM

        transform Rename::Field, from: :id_number, to: :subjectIdentifier
        transform Merge::ConstantValue, target: :subjectDocumentType, value: 'CollectionObject'

        transform Delete::Fields, fields: %i[akey m_id]
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/rel_acq-acqco.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: CSVOPT
        
        post_process do
          puts "\n\nACQITEM OBJECT-TO-ACQUISITION RELATIONSHIPS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end  
      end
      Kiba.run(acqitem_acq_rel)
    end
  end
end
