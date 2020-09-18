# frozen_string_literal: true

# initial_prep
#  - only keep data for objects being migrated
#  - flags rows with no measurement data
#  - flags various problem data, including duplicates
# duplicates
#  - prepares report of rows having duplicate data for the same object
# fractions
#  - prepares report of rows having fractions instead of floats as values
# empty
#  - prepares report of rows having no measurement data
# kept
#  - prepares problem-free data we are keeping for further processing
# reshape
#  - reshape dimension values, units, etc. closer to what is required for CollectionSpace
# derive_initial_lookup_table
#  - creates initial lookup table from reshaped data by mkey and measurements display value
# compile_dimension_subgroups
#  - join dimension subgroup values under respective measurement display values
# for_merge
#  - joins measurement field groups (with their respective dimension subgroups) for each object
#  - this data is now ready to merge into a CollectionSpace object

module Mimsy
  module Measurements
    extend self

    def initial_prep
      Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")
      
      init = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}
        @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/catalogue.tsv",
                                        csvopt: TSVOPT,
                                        keycolumn: :mkey)
        
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/measurements.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        #merge in object_id for objects we are migrating
        transform Merge::MultiRowLookup,
          fieldmap: { object_id: :id_number },
          lookup: @cat,
          keycolumn: :mkey,
          delim: MVDELIM
        transform FilterRows::FieldPopulated, action: :keep, field: :object_id

        # flag rows that have no values
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[dimension1 dimension2 dimension3 cdimension1 cdimension2 cdimension3],
          target: :valconcat,
          sep: ' ',
          delete_sources: false
        transform do |row|
          v = row.fetch(:valconcat, '')
          v = v.nil? ? '' : v
          v = v.gsub(' ', '')
          v.empty? ? row[:empty] = 'y' : row[:empty] = 'n'
          row
        end
        transform Delete::Fields, fields: %i[valconcat]

        
        transform Explode::ColumnsRemappedInNewRows,
          remap_groups: [
            %i[dimension1 unit1 dimension2 unit2 dimension3 unit3],
            %i[cdimension1 cunit1 cdimension2 cunit2 cdimension3 cunit3]
          ],
          map_to: %i[height hunit width wunit depth dunit]

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[hunit wunit dunit],
          find: '^in$',
          replace: 'inches'
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[hunit wunit dunit],
          find: '^cm$',
          replace: 'centimeters'
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[hunit wunit dunit],
          find: '^ft$',
          replace: 'feet'
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[hunit wunit dunit],
          find: '^m$',
          replace: 'meters'



        # flag rows that are duplicates
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[mkey height hunit width wunit depth dunit display],
          target: :concat,
          sep: ' ',
          delete_sources: false
        transform Deduplicate::Flag, on_field: :concat, in_field: :duplicate, using: @deduper
        transform Delete::Fields, fields: %i[concat]
        
        # flag rows that have no values
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[height width depth],
          target: :valconcat,
          sep: ' ',
          delete_sources: false
        transform do |row|
          v = row.fetch(:valconcat, '')
          v = v.nil? ? '' : v
          v = v.gsub(' ', '')
          v.empty? ? row[:remappedempty] = 'y' : row[:remappedempty] = 'n'
          row
        end

        # flag rows where values contain '/' (indicating fractions instead of decimals)
        transform Merge::ConstantValueConditional,
          fieldmap: { fraction: 'y' },
          conditions: {
            include: {
              field_include: { fieldsets: [
                {
                  includes: [
                    ['row::valconcat', 'value::/']
                  ]
                }
              ]}
            }
          }
        
        transform Delete::Fields, fields: %i[valconcat]
        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/measurements_flagged.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[mkey object_id],
          csv_options: TSVOPT
        post_process do
          puts "\n\nMEASUREMENTS FLAGGED"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(init)
    end

    def duplicates
      initial_prep unless File.file?("#{DATADIR}/working/measurements_flagged.tsv")
      
      dupes = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/measurements_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'
        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/DUPLICATE_measurements.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nDUPLICATE MEASUREMENTS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(dupes)
    end

    def fractions
      initial_prep unless File.file?("#{DATADIR}/working/measurements_flagged.tsv")

      fractionjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/measurements_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :fraction, value: 'y'
        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/FRACTION_measurements.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nMEASUREMENTS CONTAINING FRACTIONS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(fractionjob)      
    end

    def empty
      initial_prep unless File.file?("#{DATADIR}/working/measurements_flagged.tsv")
      
      emptyjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/measurements_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :empty, value: 'y'
        transform Delete::FieldsExcept, keepfields: %i[object_id display]
        #show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports/EMPTY_measurements.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nEMPTY MEASUREMENTS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(emptyjob)
    end

    def kept
      initial_prep unless File.file?("#{DATADIR}/working/measurements_flagged.tsv")
      
      keepjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/measurements_flagged.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :remappedempty, value: 'n'
        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
        transform FilterRows::FieldEqualTo, action: :reject, field: :fraction, value: 'y'
        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/measurements_keep.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nMEASUREMENTS TO KEEP"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(keepjob)
    end
    
    def reshape
      kept unless File.file?("#{DATADIR}/working/measurements_keep.tsv")
      
      reshapejob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @m = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/measurements_keep.tsv",
                                      csvopt: TSVOPT,
                                      keycolumn: :mkey)


        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/measurements_keep.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Merge::CountOfMatchingRows, lookup: @m, keycolumn: :mkey, targetfield: :rowct
        transform Delete::Fields, fields: %i[empty duplicate fraction]

        # # SECTION BELOW cleans up display column values, splitting label before a colon into its own column
        # # remove blank label (values starting with ': ')
        # transform Split::IntoMultipleColumns,
        #   field: :display,
        #   sep: ':',
        #   max_segments: 2,
        #   warnfield: :display_warning,
        #   collapse_on: :left
        # # END SECTION

        # deletes unit value if there is no corresponding dimension value
        transform do |row|
          h = {
            height: :hunit,
            width: :wunit,
            depth: :dunit,
          }
          h.each do |dim, unit|
            dv = row.fetch(dim, nil)
            row[unit] = nil if dv.nil?
          end
          row
        end

        transform Reshape::CollapseMultipleFieldsToOneTypedFieldPair,
          sourcefieldmap: {
            height: 'height',
            width: 'width',
            depth: 'depth'
          },
          datafield: :value,
          typefield: :dimension,
          targetsep: '^^'

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[hunit wunit dunit],
          target: :measurementUnit,
          sep: '^^'

        transform Delete::Fields, fields: %i[remappedempty part_measured rowct]
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/measurements_shaped.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nMEASUREMENTS SHAPED"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(reshapejob)
    end

    def derive_initial_lookup_table
      reshape unless File.file?("#{DATADIR}/working/measurements_shaped.tsv")
      
      initlkupjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/measurements_shaped.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[mkey display],
          target: :mkey_disp,
          sep: ' ',
          delete_sources: false

        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/measurements_lkup1.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nINTIAL LOOKUP TABLE"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(initlkupjob)      
    end

    def compile_dimension_subgroups
      derive_initial_lookup_table unless File.file?("#{DATADIR}/working/measurements_lkup1.tsv")
      
      combinedispjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}

        @lkup = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/measurements_lkup1.tsv",
                                         csvopt: TSVOPT,
                                         keycolumn: :mkey_disp)
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/measurements_shaped.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[mkey display],
          target: :mkey_disp,
          sep: ' ',
          delete_sources: false

        transform Deduplicate::Flag, on_field: :mkey_disp, in_field: :duplicate, using: @deduper
        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
        transform Delete::Fields, fields: %i[value dimension measurementunit duplicate]
        
        transform Merge::MultiRowLookup,
          lookup: @lkup,
          keycolumn: :mkey_disp,
          fieldmap: {
            value: :value,
            dimension: :dimension,
            measurementunit: :measurementunit
          },
          delim: '^^'

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[display],
          find: ';',
          replace: ','

        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/measurements_subgroups.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nMEASUREMENT SUBGROUPS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(combinedispjob)      
    end

    def for_merge
      compile_dimension_subgroups unless File.file?("#{DATADIR}/working/measurements_subgroups.tsv")
      
      fieldgrpjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}

        @lkup = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/measurements_subgroups.tsv",
                                         csvopt: TSVOPT,
                                         keycolumn: :mkey)
        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/measurements_shaped.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Deduplicate::Flag, on_field: :mkey, in_field: :duplicate, using: @deduper
        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
        transform Delete::Fields, fields: %i[value dimension measurementunit duplicate]
        
        transform Merge::MultiRowLookup,
          lookup: @lkup,
          keycolumn: :mkey,
          fieldmap: {
            display: :display,
            value: :value,
            dimension: :dimension,
            measurementunit: :measurementunit
          },
          delim: ';'

        #  show_me!
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/measurements_groups.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          csv_options: TSVOPT
        post_process do
          puts "\n\nMEASUREMENT GROUPS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(fieldgrpjob)      
    end
  end
end
