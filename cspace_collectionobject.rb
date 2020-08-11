require_relative 'config'
require_relative 'prelim_acqitems'
require_relative 'prelim_cat'
require_relative 'prelim_place'
require_relative 'prelim_measurement_prepare'
require_relative 'prelim_inscription'
require_relative 'prelim_concept'
require_relative 'prelim_names_for_co'

Mimsy::AcqItems.setup
Mimsy::Cat.setup
Mimsy::Place.setup
Mimsy::Measurements.setup
Mimsy::Inscription.setup
Mimsy::Concept.setup
Mimsy::NamesForCollectionObject.setup

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
  @subjects = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/concept_item_lookup.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :mkey)
  @places = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/place_norm_lookup.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :normplace)

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

  # SECTION BELOW ensures place format matching place authorities is used in object records
  #  This is because I ran into trouble importing objects whose stub place records resolved
  #  to the same normalized CSpace ID
  transform Cspace::NormalizeForID, source: :place_collected, target: :norm_place_collected
  transform Cspace::NormalizeForID, source: :place_made, target: :norm_place_made

  transform Merge::MultiRowLookup,
    lookup: @places,
    keycolumn: :norm_place_collected,
    fieldmap: {
      :fieldCollectionPlaceLocal => :place,
    },
    conditions: {
      include: {
        :field_equal => { fieldsets: [
          {matches: [
            ['mergerow::duplicate','value::n']
          ]}
        ]}
      }
    },
    delim: MVDELIM
  
  transform Merge::MultiRowLookup,
    lookup: @places,
    keycolumn: :norm_place_made,
    fieldmap: {
      :objectProductionPlaceLocal => :place,
    },
    conditions: {
      include: {
        :field_equal => { fieldsets: [
          {matches: [
            ['mergerow::duplicate','value::n']
          ]}
        ]}
      }
    },
    delim: MVDELIM

  transform do |row|
    pc = row.fetch(:place_collected, nil)
    pcn = row.fetch(:fieldCollectionPlaceLocal, nil)
    row[:pcdiff] = pc == pcn ? nil : 'y'

    pm = row.fetch(:place_made, nil)
    pmn = row.fetch(:objectProductionPlaceLocal, nil)
    row[:pmdiff] = pm == pmn ? nil : 'y'
    row
  end

    transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[pcdiff pmdiff],
    target: :concat,
    sep: ' ',
    delete_sources: false

  transform Delete::Fields, fields: %i[place_collected place_made norm_place_collected norm_place_made pcdiff pmdiff concat]

  #transform Rename::Field, from: :place_collected, to: :fieldCollectionPlaceLocal
  #transform Rename::Field, from: :place_made, to: :objectProductionPlaceLocal
  # END SECTION
  
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
      contentConceptAssociated: :migratingsub
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
  transform Merge::ConstantValue, target: :datasource, value: 'cat'

  transform Delete::Fields, fields: %i[duplicate]

#  show_me!
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/working/collectionobjects_uniq.tsv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT
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

combineuniq = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/collectionobjects_uniq.tsv", csv_options: TSVOPT
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_collectionobjects.tsv", csv_options: TSVOPT
   # transform{ |r| @srcrows += 1; r }
  #transform{ |r| r }
  #show_me!
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform Merge::ConstantValueConditional,
  fieldmap: {
    collection: nil,
    comment: nil,
    contentconceptassociated: nil,
    dimension: nil,
    dimensionsummary: nil,
    fieldcollectiondategroup: nil,
    fieldcollectionplacelocal: nil,
    inscriptioncontent: nil,
    inscriptioncontentlanguage: nil,
    inscriptioncontenttranslation: nil,
    inscriptioncontenttype: nil,
    inscriptioncontentmethod: nil,
    inscriptioncontentposition: nil,
    inscriptioncontentinterpretation: nil,
    limitationlevel: nil,
    limitationtype: nil,
    material: nil,
    measurementunit: nil,
    namedcollection: nil,
    numberofobjects: nil,
    objectname: nil,
    objectnamelanguage: nil,
    objectproductiondategroup: nil,
    objectproductionorganization: nil,
    objectproductionorganizationrole: nil,
    objectproductionpeople: nil,
    objectproductionperson: nil,
    objectproductionpersonrole: nil,
    objectproductionplacelocal: nil,
    publishto: nil,
    title: nil,
    value: nil
  },
  conditions: {
    include: {
      field_equal: { fieldsets: [
        {
          type: :any,
          matches: [
            ['row::datasource', 'value::acqitem']
          ]
        }
      ]}
    }
  }

transform Clean::RegexpFindReplaceFieldVals,
  fields: %i[briefdescription comment],
  find: 'LINEBREAKWASHERE',
  replace: "\n"

transform Delete::Fields, fields: %i[datasource]
#show_me!
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/collectionobjects.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: CSVOPT
  post_process do
    puts "\n\nCS COLLECTIONOBJECTS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(combineuniq)
