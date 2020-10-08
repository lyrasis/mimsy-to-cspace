# frozen_string_literal: true

# flag
# - flag duplicate objectnumbers
# - keep only rows with objectnumbers
# duplicates
#  - report duplicate objects that will not be migrated
# unique
#  - write unique objects
# build
#  - build collectionobjects out of working catalogue and all other sources
# csv
#  - combine unique objects created from build with uncataloged items created from
#    acquisition_items
# hierarchy
#  - create importable CSV of hierarchical object relationships
# related
#  - create importable CSV of non-hierarchical object relationships
module Cspace
  module CollectionObject
    extend self

    def flag
        Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")
      flagjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # id_number is required
        transform FilterRows::FieldPopulated, action: :keep, field: :id_number

        transform Rename::Field, from: :id_number, to: :objectnumber

        transform Deduplicate::Flag, on_field: :objectnumber, in_field: :duplicate, using: @deduper
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/collectionobjects_duplicates_flagged.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[objectnumber],
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nOBJECT RECORDS WITH DUPLICATES FLAGGED"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(flagjob)
    end

    def unique
      flag unless File.file?("#{DATADIR}/working/collectionobjects_duplicates_flagged.tsv")
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
    end

    def duplicates
      flag unless File.file?("#{DATADIR}/working/collectionobjects_duplicates_flagged.tsv")
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
    end

    def build
      unique unless File.file?("#{DATADIR}/working/collectionobjects_uniq.tsv")
      Mimsy::ItemsMakers.for_merge unless File.file?("#{DATADIR}/working/items_makers.tsv")
      Mimsy::Measurements.for_merge unless File.file?("#{DATADIR}/working/measurements_groups.tsv")
      Mimsy::Inscription.for_merge unless File.file?("#{DATADIR}/working/inscription.tsv")
      Mimsy::ItemNames.prep unless File.file?("#{DATADIR}/working/item_names.tsv")
      Mimsy::Place.normalized_place_lookup unless File.file?("#{DATADIR}/working/place_norm_lookup.tsv")
      Mimsy::Subject.item_subject_lookup unless File.file?("#{DATADIR}/working/subject_item_lookup.tsv")
      Cspace::Work.lookup unless File.file?("#{DATADIR}/working/works_lookup.tsv")
      Mimsy::Notepad.prep unless File.file?("#{DATADIR}/working/notepad.tsv")
      
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
        @places = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/place_norm_lookup.tsv",
                                           csvopt: TSVOPT,
                                           keycolumn: :normplace)
        @inames = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/item_names.tsv",
                                           csvopt: TSVOPT,
                                           keycolumn: :mkey)
        @colls = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/works_lookup.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :norm_coll)
        @notes = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/notepad.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :mkey)
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/collectionobjects_uniq.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # SECTION BELOW merges in acquisition date
        transform Merge::MultiRowLookup,
          lookup: @acqitems,
          keycolumn: :mkey,
          fieldmap: {
            :assocstructureddategroup => :transfer_date,
          },
          constantmap: {
            :assocdatetype => 'acquisition transfer date'
          },
          delim: MVDELIM
        # END SECTION

        # SECTION BELOW merges in item_name data and keeps any non-duplicate info
        transform Rename::Field, from: :item_name, to: :objectname
        transform Copy::Field, from: :objectname, to: :origobjname
        transform Merge::MultiRowLookup,
          lookup: @inames,
          keycolumn: :mkey,
          fieldmap: {
            :inameval => :item_name
          },
          delim: MVDELIM
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[objectname inameval],
          target: :objectname,
          sep: ';',
          delete_sources: false
        transform Clean::DowncaseFieldValues, fields: %i[objectname]
        transform do |row|
          objname = row.fetch(:objectname, nil)
          if objname.blank?
            row
          else
            objname = objname[';'] ? objname.split(';').map(&:strip).join(';') : objname
            row[:objectname] = objname
            row
          end
        end
        transform Deduplicate::FieldValues, fields: %i[objectname], sep: ';'
        # END SECTION
        
        # SECTION BELOW merges in typed objectProductionPerson or objectProductionOrganization
        #  values and associated roles
        transform Merge::MultiRowLookup,
          lookup: @names,
          keycolumn: :maker,
          fieldmap: {
            :makerperson => :preferred_name,
          },
          constantmap: {
            :makerpersonrole => 'maker'
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
            :makerorganization => :preferred_name,
          },
          constantmap: {
            :makerorganizationrole => 'maker'
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
            :objectproductionperson => :preferred_name,
            :objectproductionpersonrole => :role
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
            :objectproductionorganization => :preferred_name,
            :objectproductionorganizationrole => :role
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
            objectproductionperson: %i[makerperson objectproductionperson],
            objectproductionpersonrole: %i[makerpersonrole objectproductionpersonrole]
          },
          sep: MVDELIM
        transform Deduplicate::GroupedFieldValues,
          on_field: :objectproductionperson,
          grouped_fields: %i[objectproductionpersonrole],
          sep: MVDELIM
        
        transform CombineValues::AcrossFieldGroup,
          fieldmap: {
            objectproductionorganization: %i[makerorganization objectproductionorganization],
            objectproductionorganizationrole: %i[makerorganizationrole objectproductionorganizationrole]
          },
          sep: MVDELIM
        transform Deduplicate::GroupedFieldValues,
          on_field: :objectproductionorganization,
          grouped_fields: %i[objectproductionorganizationrole],
          sep: MVDELIM
        # END SECTION
        
        transform Rename::Field, from: :description, to: :briefdescription

        transform do |row|
          on = row.fetch(:objectname, nil)
          if on.blank?
            row[:objectnamelanguage] = nil
          else
            val = on[';'] ? on.split(';').map{ |e| 'English' }.join(';') : 'English'
            row[:objectnamelanguage] = val
          end
          row
        end
        # this isn't working for some reason I don't have time to fix...
        # transform Merge::ConstantValueConditional,
        #   fieldmap: {objectnamelanguage: 'English'},        #   conditions: {
        #     exclude: {
        #       field_empty: {
        #         fieldsets: [
        #           {fields: %w[row::objectname]}
        #         ]
        #       }
        #     }
        #   }

        transform Rename::Field, from: :item_count, to: :numberofobjects
        transform Rename::Field, from: :materials, to: :material
        transform Rename::Field, from: :date_collected, to: :fieldcollectiondategroup
        transform Rename::Field, from: :culture, to: :objectproductionpeople
        transform Rename::Field, from: :date_made, to: :objectproductiondategroup
        transform Rename::Field, from: :language_of_material, to: :contentlanguage
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[contentlanguage],
          find: 'eng',
          replace: 'English'
        
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

        transform Merge::MultiRowLookup,
          lookup: @notes,
          keycolumn: :mkey,
          fieldmap: {
            :morecomments => :value,
          }

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[comment morecomments],
          target: :comment,
          sep: MVDELIM

        transform Cspace::NormalizeForID, source: :credit_line, target: :norm_credit
        transform Merge::MultiRowLookup,
          lookup: @colls,
          keycolumn: :norm_credit,
          fieldmap: {
            :namedcollection => :use_value,
          },
          delim: MVDELIM
        transform Delete::Fields, fields: %i[credit_line norm_credit]

        # SECTION BELOW ensures place format matching place authorities is used in object records
        #  This is because I ran into trouble importing objects whose stub place records resolved
        #  to the same normalized CSpace ID
        transform Cspace::NormalizeForID, source: :place_collected, target: :norm_place_collected
        transform Cspace::NormalizeForID, source: :place_made, target: :norm_place_made

        transform Merge::MultiRowLookup,
          lookup: @places,
          keycolumn: :norm_place_collected,
          fieldmap: {
            :fieldcollectionplacelocal => :place,
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
            :objectproductionplacelocal => :place,
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
          pcn = row.fetch(:fieldcollectionplacelocal, nil)
          row[:pcdiff] = pc == pcn ? nil : 'y'

          pm = row.fetch(:place_made, nil)
          pmn = row.fetch(:objectproductionplacelocal, nil)
          row[:pmdiff] = pm == pmn ? nil : 'y'
          row
        end

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[pcdiff pmdiff],
          target: :concat,
          sep: ' ',
          delete_sources: false

        transform Delete::Fields, fields: %i[place_collected place_made norm_place_collected norm_place_made pcdiff pmdiff concat]

        #transform Rename::Field, from: :place_collected, to: :fieldcollectionplacelocal
        #transform Rename::Field, from: :place_made, to: :objectproductionplacelocal
        # END SECTION
        
        transform Merge::MultiRowLookup,
          lookup: @measure,
          keycolumn: :mkey,
          fieldmap: {
            gdimensionsummary: :display,
            gvalue: :value,
            gdimension: :dimension,
            gmeasurementunit: :measurementunit
          }
        transform Rename::Field, from: :measurements, to: :dimensionsummary

        transform Merge::MultiRowLookup,
          lookup: @inscription,
          keycolumn: :mkey,
          fieldmap: {
            inscriptioncontent: :inscriptioncontent,
            inscriptioncontentlanguage: :inscriptioncontentlanguage,
            inscriptioncontenttranslation: :inscriptioncontenttranslation,
            inscriptioncontenttype: :inscriptioncontenttype,
            inscriptioncontentmethod: :inscriptioncontentmethod,
            inscriptioncontentposition: :inscriptioncontentposition,
            inscriptioncontentinterpretation: :inscriptioncontentinterpretation
          }

        transform Merge::MultiRowLookup,
          lookup: @subjects,
          keycolumn: :mkey,
          fieldmap: {
            contentconceptassociated: :migratingsub
          },
          delim: MVDELIM

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[dimensionsummary],
          find: ';',
          replace: ','

        transform do |row|
          gd = row.fetch(:gdimensionsummary, nil)
          d = row.fetch(:dimensionsummary, nil)
          if gd.nil?
            row[:gdimensionsummary] = row[:dimensionsummary] if d
          end
          row
        end

        transform Delete::Fields, fields: %i[dimensionsummary]

        transform Rename::Field, from: :gdimensionsummary, to: :dimensionsummary
        transform Rename::Field, from: :gvalue, to: :value
        transform Rename::Field, from: :gdimension, to: :dimension
        transform Rename::Field, from: :gmeasurementunit, to: :measurementunit

        transform Merge::ConstantValueConditional,
          fieldmap: {
            limitationtype: 'lending',
            limitationlevel: 'restriction'
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
          target: :publishto,
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
            objectname: 'Manuscript',
            objectnamelanguage: 'English'
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
          target: :inventorystatus,
          mapping: INVSTATUS,
          fallback_val: :nil
        # END SECTION

        transform Delete::Fields, fields: %i[mkey category1 legal_status id_number_cat maker offsite
                                             system_count parent_key broader_text whole_part
                                             home_location loan_allowed offsite location location_date
                                             location_levels item_name origobjname inameval]

        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/collectionobjects_main.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[objectnumber comment],
          csv_options: TSVOPT
        
        post_process do
          puts "\n\nOBJECT RECORDS FROM CATALOGUE"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(catjob)
    end
    
    def csv
      build unless File.file?("#{DATADIR}/working/collectionobjects_main.tsv")
      Mimsy::AcqItems.uncat_items unless File.file?("#{DATADIR}/working/acqitem_collectionobjects.tsv")
      
      combineuniq = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/collectionobjects_main.tsv", csv_options: TSVOPT
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/acqitem_collectionobjects.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Merge::ConstantValueConditional,
          fieldmap: {
            collection: nil,
            comment: nil,
            contentconceptassociated: nil,
            contentlanguage: nil,
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
            inventorystatus: 'not cataloged',
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

        #show_me!
        
        transform do |row|
          bd = row.fetch(:briefdescription, nil)
          row[:briefdescription] = nil if bd.blank?
          row
        end

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[briefdescription comment],
          find: 'LINEBREAKWASHERE',
          replace: "\n"

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[briefdescription comment],
          find: 'TABCHARACTERWASHERE',
          replace: "     "

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[briefdescription],
          find: ';',
          replace: ','
        

        transform Delete::Fields, fields: %i[datasource]
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
    end

    def hierarchy
      Mimsy::Cat.build_relationships unless File.file?("#{DATADIR}/working/rels_co-co_data.tsv")

      relspopulated = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/rels_co-co_data.tsv", csv_options: TSVOPT
        # Ruby's CSV gives us "CSV::Row" but we want Hash
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :keep, field: :parent_id

        transform Merge::ConstantValue, target: :type, value: 'CollectionObject'
        transform Delete::FieldsExcept, keepfields: %i[type id_number parent_id]
        transform Rename::Field, from: :id_number, to: :narrower
        transform Rename::Field, from: :parent_id, to: :broader
        
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/rel_co-co_hierarchy.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: CSVOPT
        
        post_process do
          puts "\n\nCOLLECTIONOBJECT HIERARCHY"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(relspopulated)
    end

    def related
      Mimsy::RelatedItem.prep unless File.file?("#{DATADIR}/working/rels_co-co_non-hier.tsv")

      rels = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/rels_co-co_non-hier.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        # objectIdentifier	objectDocumentType	subjectIdentifier	subjectDocumentType

        transform Merge::ConstantValue, target: :objectdocumenttype, value: 'CollectionObject'
        transform Merge::ConstantValue, target: :subjectdocumenttype, value: 'CollectionObject'
        transform Rename::Field, from: :subject_id, to: :subjectidentifier
        transform Rename::Field, from: :object_id, to: :objectidentifier
        
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/rel_co-co.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[subjectidentifier subjectdocumenttype objectidentifier objectdocumenttype],
          csv_options: CSVOPT
        
        post_process do
          puts "\n\nCOLLECTIONOBJECT RELATED"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(rels)
    end
  end
end
