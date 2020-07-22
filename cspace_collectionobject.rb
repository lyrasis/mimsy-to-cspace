require_relative 'config'
require_relative 'prelim_cat'
require_relative 'prelim_measurement_prepare'
require_relative 'prelim_inscription'
require_relative 'prelim_concept'

Mimsy::Cat.setup
Mimsy::Measurements.setup
Mimsy::Inscription.setup
Mimsy::Concept.setup

# creates working copy of items_makers with preferred_name & individual columns merged in from people,
#  role column inserted based on relationship, affiliation, and prior attribution
namesjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0
  @names = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people.tsv",
                                    csvopt: TSVOPT,
                                    keycolumn: :link_id)

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/items_makers.tsv", csv_options: TSVOPT
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform Merge::MultiRowLookup,
    lookup: @names,
    keycolumn: :link_id,
    fieldmap: {
      :preferred_name => :preferred_name,
      :individual => :individual
    }

  # where affiliation = Maker, relationship is blank --- collapse into one downcased column
  transform Rename::Field, from: :relationship, to: :role
  transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[role affiliation],
    target: :role,
    sep: ''
  transform Clean::DowncaseFieldValues, fields: [:role]

  # turn "maker" into "maker (prior attribution)" if prior_attribution column = Y
  transform Replace::FieldValueWithStaticMapping,
    source: :prior_attribution,
    target: :prior_attribution_mapped,
    mapping: PRIORATTR,
    delete_source: false
  transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[role prior_attribution_mapped],
    target: :role,
    sep: ''

  #show_me!
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/items_makers.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
  post_process do
    puts "\n\nITEMS_MAKERS COPY"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(namesjob)

acqitemcatjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/acquisition_items.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  # this only processes rows with no link to catalogue.csv
  transform FilterRows::FieldPopulated, action: :reject, field: :m_id
  
  # id_number is required
  transform FilterRows::FieldPopulated, action: :keep, field: :id_number


  transform Rename::Field, from: :id_number, to: :objectNumber
  transform Rename::Field, from: :transfer_date, to: :assocstructureddategroup
  transform Merge::ConstantValueConditional,
    fieldmap: { assocdatetype: 'acquisition transfer date' },
    conditions: {
      exclude: {
        field_empty: { fieldsets: [
          {
            fields: ['row::assocstructureddategroup']
          }
        ]
       }
      }
    }
  transform Merge::ConstantValue, target: :inventoryStatus, value: 'not cataloged'


  transform do |row|
    summary = row.fetch(:item_summary, nil)
    if summary
      row[:briefDescription] = summary.split('LINEBREAKWASHERE').join(' -- ')
    else
      row[:briefDescription] = nil
    end
    row
  end

  # # SECTION below adds columns used in collectionobject csv
  # # It is unnecessary if we can't combine two data sources
  # transform Merge::ConstantValue, target: :title, value: nil
  # transform Merge::ConstantValue, target: :objectproductionperson, value: nil
  # transform Merge::ConstantValue, target: :objectproductionorganization, value: nil
  # transform Merge::ConstantValue, target: :objectproductionpersonrole, value: nil
  # transform Merge::ConstantValue, target: :objectproductionorganizationrole, value: nil
  # transform Merge::ConstantValue, target: :objectname, value: nil
  # transform Merge::ConstantValue, target: :objectnamelanguage, value: nil
  # transform Merge::ConstantValue, target: :numberofobjects, value: nil
  # transform Merge::ConstantValue, target: :material, value: nil
  # transform Merge::ConstantValue, target: :fieldcollectiondategroup, value: nil
  # transform Merge::ConstantValue, target: :fieldcollectionplacelocal, value: nil
  # transform Merge::ConstantValue, target: :objectproductionplacelocal, value: nil
  # transform Merge::ConstantValue, target: :objectproductionpeople, value: nil
  # transform Merge::ConstantValue, target: :objectproductiondategroup, value: nil
  # transform Merge::ConstantValue, target: :comment, value: nil
  # transform Merge::ConstantValue, target: :namedcollection, value: nil
  # transform Merge::ConstantValue, target: :dimensionsummary, value: nil
  # transform Merge::ConstantValue, target: :limitationtype, value: nil
  # transform Merge::ConstantValue, target: :limitationlevel, value: nil
  # transform Merge::ConstantValue, target: :collection, value: nil
  # transform Merge::ConstantValue, target: :publishto, value: nil
  # # END SECTION


  transform Deduplicate::Flag, on_field: :objectNumber, in_field: :duplicate, using: @deduper

  transform Delete::Fields, fields: %i[id akey m_id item_summary status status_date accession_date
                                       title_transfer_requested total_cost value_currency item_marked
                                       reproduction_requested note catalogued]

#  show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/acqitem_collectionobjects_duplicates_flagged.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    initial_headers: %i[objectNumber],
    csv_options: TSVOPT
    
  post_process do
    puts "\n\nOBJECT RECORDS FROM ACQ ITEMS WITHOUT CAT (DUPLICATES FLAGGED)"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(acqitemcatjob)

uniqacqcatjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_collectionobjects_duplicates_flagged.tsv", csv_options: TSVOPT
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'

  transform Delete::Fields, fields: %i[duplicate]

#  show_me!
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/acqcat_collectionobjects.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
  post_process do
    puts "\n\nUNIQUE OBJECT RECORDS FROM ACQITEMS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(uniqacqcatjob)

dupeacqcatjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_collectionobjects_duplicates_flagged.tsv", csv_options: TSVOPT
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'

  transform Delete::Fields, fields: %i[duplicate]

#  show_me!
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/reports/DUPLICATE_acqitems_mapped_as_objects.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
  post_process do
    puts "\n\nDUPLICATE OBJECT RECORDS FROM ACQITEMS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(dupeacqcatjob)

# create cspace collectionobject records
catjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0


  @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :m_id)
  @makers = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/items_makers.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :mkey)
  @names = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/people.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :preferred_name)
  @measure = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/measurements_groups.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :mkey)
  @inscription = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/inscription.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :mkey)
  @subjects = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/subject_item_lookup.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :mkey)

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

#  transform FilterRows::FieldEqualTo, action: :keep, field: :mkey, value: '1113'

  # id_number is required
  transform FilterRows::FieldPopulated, action: :keep, field: :id_number

  transform Merge::MultiRowLookup,
    lookup: @acqitems,
    keycolumn: :mkey,
    fieldmap: {
      :assocStructuredDateGroup => :transfer_date,
    },
    constantmap: {
      :assocDateType => 'acquisition transfer date'
    },
    delim: MVDELIM
  # END SECTION

  # SECTION BELOW merges in typed objectProductionPerson or objectProductionOrganization
  #  values and associated roles
  transform Merge::MultiRowLookup,
    lookup: @names,
    keycolumn: :maker,
    fieldmap: {
      :makerPerson => :preferred_name,
    },
    constantmap: {
      :makerPersonRole => 'maker'
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
    lookup: @names,
    keycolumn: :maker,
    fieldmap: {
      :makerOrganization => :preferred_name,
    },
    constantmap: {
      :makerOrganizationRole => 'maker'
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
    lookup: @makers,
    keycolumn: :mkey,
    fieldmap: {
      :objectProductionPerson => :preferred_name,
      :objectProductionPersonRole => :role
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
    lookup: @makers,
    keycolumn: :mkey,
    fieldmap: {
      :objectProductionOrganization => :preferred_name,
      :objectProductionOrganizationRole => :role
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

  transform CombineValues::AcrossFieldGroup,
    fieldmap: {
      objectProductionPerson: %i[makerPerson objectProductionPerson],
      objectProductionPersonRole: %i[makerPersonRole objectProductionPersonRole]
    },
    sep: MVDELIM
  transform Deduplicate::GroupedFieldValues,
    on_field: :objectProductionPerson,
    grouped_fields: %i[objectProductionPersonRole],
    sep: MVDELIM
  
   transform CombineValues::AcrossFieldGroup,
     fieldmap: {
       objectProductionOrganization: %i[makerOrganization objectProductionOrganization],
       objectProductionOrganizationRole: %i[makerOrganizationRole objectProductionOrganizationRole]
     },
     sep: MVDELIM
  transform Deduplicate::GroupedFieldValues,
    on_field: :objectProductionOrganization,
    grouped_fields: %i[objectProductionOrganizationRole],
    sep: MVDELIM
  #END SECTION
  
  transform Rename::Field, from: :id_number, to: :objectNumber
  transform Rename::Field, from: :description, to: :briefDescription

  transform Rename::Field, from: :item_name, to: :objectName
  transform Merge::ConstantValueConditional,
    fieldmap: {objectNameLanguage: 'English'},
    conditions: {
      exclude: {
        field_empty: {
          fieldsets: [
            {fields: %w[row::objectName]}
          ]
        }
      }
    }

  transform Rename::Field, from: :item_count, to: :numberOfObjects
  transform Rename::Field, from: :materials, to: :material
  transform Rename::Field, from: :date_collected, to: :fieldCollectionDateGroup
  transform Rename::Field, from: :place_collected, to: :fieldCollectionPlaceLocal
  transform Rename::Field, from: :place_made, to: :objectProductionPlaceLocal
  transform Rename::Field, from: :culture, to: :objectProductionPeople
  transform Rename::Field, from: :date_made, to: :objectProductionDateGroup
  
  transform Rename::Field, from: :note, to: :comment1
  transform Rename::Field, from: :option4, to: :comment2
  transform Clean::RegexpFindReplaceFieldVals,
    fields: %i[comment1 comment2],
    find: ';',
    replace: ', '
  transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[comment1 comment2],
    target: :comment,
    sep: ' --- '

  transform Rename::Field, from: :credit_line, to: :namedCollection

  transform Merge::MultiRowLookup,
    lookup: @measure,
    keycolumn: :mkey,
    fieldmap: {
      gDimensionSummary: :display,
      gValue: :value,
      gDimension: :dimension,
      gMeasurementUnit: :measurementunit
    }
  transform Rename::Field, from: :measurements, to: :dimensionSummary

  transform Merge::MultiRowLookup,
    lookup: @inscription,
    keycolumn: :mkey,
    fieldmap: {
      inscriptionContent: :inscriptioncontent,
      inscriptionContentLanguage: :inscriptioncontentlanguage,
      inscriptionContentTranslation: :inscriptioncontenttranslation,
      inscriptionContentType: :inscriptioncontenttype,
      inscriptionContentMethod: :inscriptioncontentmethod,
      inscriptionContentPosition: :inscriptioncontentposition,
      inscriptionContentInterpretation: :inscriptioncontentinterpretation
    }

  transform Merge::MultiRowLookup,
    lookup: @subjects,
    keycolumn: :mkey,
    fieldmap: {
      contentConcept: :migratingsub
    },
    delim: MVDELIM

  transform Clean::RegexpFindReplaceFieldVals,
    fields: %i[dimensionSummary],
    find: ';',
    replace: ','

  transform do |row|
    gd = row.fetch(:gDimensionSummary, nil)
    d = row.fetch(:dimensionSummary, nil)
    if gd.nil?
      row[:gDimensionSummary] = row[:dimensionSummary] if d
    end
    row
  end

  transform Delete::Fields, fields: %i[dimensionSummary]

  transform Rename::Field, from: :gDimensionSummary, to: :dimensionSummary
  transform Rename::Field, from: :gValue, to: :value
  transform Rename::Field, from: :gDimension, to: :dimension
  transform Rename::Field, from: :gMeasurementUnit, to: :measurementUnit

  transform Merge::ConstantValueConditional,
    fieldmap: {
      limitationType: 'lending',
      limitationLevel: 'restriction'
    },
    conditions: {
      include: {
        field_equal: { fieldsets: [
          {
            matches: [
              ['row::loan_allowed', 'value::N']
            ]
          }
        ]}
      }
    }

  transform Merge::ConstantValueConditional,
    fieldmap: {
      collection: 'permanent-collection'
    },
    conditions: {
      include: {
        field_equal: { fieldsets: [
          {
            matches: [
              ['row::legal_status', 'value::PERMANENT COLLECTION']
            ]
          }
        ]}
      }
    }

  transform Replace::FieldValueWithStaticMapping,
    source: :publish,
    target: :publishTo,
    mapping: PUBLISH

  # SECTION BELOW cleans up category1 values
  transform Clean::RegexpFindReplaceFieldVals,
    fields: %i[category1],
    find: 'ACC+ESS+ION',
    replace: 'ACCESSION'

  transform Clean::RegexpFindReplaceFieldVals,
    fields: %i[category1],
    find: 'DETAL|DETAIl',
    replace: 'DETAIL'
  
    transform Merge::ConstantValueConditional,
    fieldmap: {
      objectName: 'Manuscript',
      objectNameLanguage: 'English'
    },
    conditions: {
      include: {
        field_equal: { fieldsets: [
          {
            matches: [
              ['row::category1', 'value::Manuscript']
            ]
          }
        ]}
      }
    }
  transform Delete::FieldValueMatchingRegexp,
    fields: [:category1],
    match: '^Manuscript$'
  transform Delete::FieldValueMatchingRegexp,
    fields: [:category1],
    match: '^Gelatin silver print$'

  transform Merge::ConstantValueConditional,
    fieldmap: {
      category1: 'ACCESSION DETAIL'
    },
    conditions: {
      include: {
        field_equal: { fieldsets: [
          {
            matches: [
              ['row::category1', 'revalue::\d{2}-\d{3}']
            ]
          }
        ]}
      }
    }

  transform Replace::FieldValueWithStaticMapping,
    source: :category1,
    target: :inventoryStatus,
    mapping: INVSTATUS,
    fallback_val: :nil


  # END SECTION

  transform Deduplicate::Flag, on_field: :objectNumber, in_field: :duplicate, using: @deduper

  transform Delete::Fields, fields: %i[mkey category1 legal_status id_number_cat maker offsite system_count parent_key broader_text whole_part
                                       home_location loan_allowed offsite location location_date location_levels]

  #show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/collectionobjects_duplicates_flagged.tsv"
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

uniqcatjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/collectionobjects_duplicates_flagged.tsv", csv_options: TSVOPT
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'

  transform Delete::Fields, fields: %i[duplicate]

#  show_me!
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/cat_collectionobjects.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
  post_process do
    puts "\n\nUNIQUE OBJECT RECORDS FROM CATALOGUE"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(uniqcatjob)

dupecatjob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/collectionobjects_duplicates_flagged.tsv", csv_options: TSVOPT
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'

  transform Delete::Fields, fields: %i[duplicate]

#  show_me!
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/reports/DUPLICATE_catalogue.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
  post_process do
    puts "\n\nDUPLICATE OBJECT RECORDS FROM CATALOGUE"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(dupecatjob)

# combineuniq = Kiba.parse do
#   extend Kiba::Common::DSLExtensions::ShowMe
#   @srcrows = 0
#   @outrows = 0

#   source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/cat_collectionobjects.tsv", csv_options: TSVOPT
#   transform { |r| r.to_h }
#  # transform{ |r| @srcrows += 1; r }
#   #transform{ |r| r }
# show_me!
#   source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqcat_collectionobjects.tsv", csv_options: TSVOPT
#   transform { |r| r.to_h }
#   transform{ |r| @srcrows += 1; r }
#   transform{ |r| r }
  
#   filename = "#{DATADIR}/cs/collectionobjects.csv"
#   destination Kiba::Extend::Destinations::CSV,
#     filename: filename,
#     csv_options: CSVOPT
#   post_process do
#     puts "\n\nCS COLLECTIONOBJECTS"
#     puts "#{@outrows} (of #{@srcrows})"
#     puts "file: #{filename}"
#   end
# end
# Kiba.run(combineuniq)
