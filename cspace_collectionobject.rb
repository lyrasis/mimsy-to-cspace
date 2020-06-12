require_relative 'config'

# create cspace acquisitions records
catjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0


  @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :m_id)

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/catalogue.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  # id_number is required
  transform FilterRows::FieldPopulated, action: :keep, field: :id_number

  # transform Merge::MultiRowLookup,
  #   lookup: @acq,
  #   keycolumn: :akey,
  #   fieldmap: {
  #     :approvalStatus => :status,
  #     :approvalDate => :status_date
  #   },
  #   constantmap: {
  #     :approvalIndividual => ''
  #   },
  #   delim: MVDELIM

  transform Rename::Field, from: :id_number, to: :objectNumber
  transform Rename::Field, from: :description, to: :briefDescription
  transform Rename::Field, from: :item_name, to: :objectName

    transform Merge::ConstantValueConditional,
    target: :objectNameLanguage,
    value: 'English',
    conditions: {
      :fields_populated => %i[objectName]
    }

  transform Rename::Field, from: :item_count, to: :numberOfObjects
  transform Rename::Field, from: :materials, to: :material
  transform Rename::Field, from: :date_collected, to: :fieldCollectionDateGroup
  transform Rename::Field, from: :place_collected, to: :fieldCollectionPlace

  transform Rename::Field, from: :note, to: :comment1
  transform Rename::Field, from: :option4, to: :comment2
  transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[comment1 comment2],
    target: :comment,
    sep: MVDELIM

  transform Rename::Field, from: :credit_line, to: :collection
  transform Rename::Field, from: :measurements, to: :dimensionSummary

  transform Rename::Field, from: :loan_allowed, to: :limitationType
  transform Merge::ConstantValueConditional,
    fieldmap: {
      :limitationType => 'lending',
      :limitationLevel => 'restriction'
    },
    conditions: {
      include: {
        field_equal: { fieldsets: [
          {matches: ['row::loan_allowed', 'value::N']}
        ]}
      }
    }

  

  transform Deduplicate::Flag, on_field: :objectNumber, in_field: :duplicate, using: @deduper

#  transform Delete::Fields, fields: %i[akey status requested_by request_date legal_date total_requested
#                                       external_file aiTransferDate]

  show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = 'data/working/acquisitions_duplicates_flagged.tsv'
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    initial_headers: %i[objectNumber],
    csv_options: TSVOPT
    
  post_process do
    puts "\n\nOBJECT RECORDS WITH DUPLICATES FLAGGED"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(catjob)
