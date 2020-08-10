require_relative 'config'
require_relative 'prelim_cat'

module Mimsy
  module Location
    # SOURCES
    # norm_location_mapping.tsv -- normValuesToNew tab of locations_working.xlsx, manually converted to TSV
    # loc_hier_lookup.tsv -- locHierToAuthorityName tab of locations_working.xlsx, manually converted to TSV
    # PROCESSES
    # loc_lookup_initial - remove extraneous columns from norm_location_mapping.tsv and save as
    #   loc_lookup_initial.tsv
    # obj_prepare - pare down catalogue.csv to only ID and two location columns.
    #   Removes rows with no location data.
    #   Adds normalized location columns (normloc normhomeloc)
    #   Merges in newloc, newhome, locnote, and homenote from loc_lookup_initial.tsv
    #   Concatenates newloc and new home in one column for testing
    # unmapped_loc_report - writes out report of rows from obj_prepare where testing column is blank.
    # obj_locs_clean
    #   Includes only rows from obj_prepare where testing column is populated
    #   Includes only columns that will be used in preparing LMI
    def self.loc_lookup
      @loc_lookup_initial = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/provided/norm_location_mapping.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Delete::FieldsExcept, keepfields: %i[norm_value loc_auth map_to_note]
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/loc_lookup_initial.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          label = 'prelim_locations/loc_lookup_initial : create locations lookup'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@loc_lookup_initial)
    end

    def self.obj_prepare
      Mimsy::Cat.setup
      Mimsy::Location.loc_lookup
      @obj_prepare = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        @loc = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/loc_lookup_initial.tsv",
                                         csvopt: TSVOPT,
                                         keycolumn: :norm_value)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/catalogue.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Delete::FieldsExcept, keepfields: %i[id_number home_location location]

        # SECTION BELOW removes rows with no location data
        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[home_location location],
          target: :concat,
          sep: ' ',
          delete_sources: false
        transform FilterRows::FieldPopulated, action: :keep, field: :concat
        transform Delete::Fields, fields: %i[concat]
        # END SECTION

        # SECTION BELOW adds normalized location columns
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[home_location location],
          find: ',',
          replace: '',
          debug: true
        transform Rename::Field, from: :home_location_repl, to: :normhomeloc
        transform Rename::Field, from: :location_repl, to: :normloc
        transform Clean::DowncaseFieldValues, fields: %i[normhomeloc normloc]
        # END SECTION

        # SECTION BELOW merges in remapped location values and location notes
        transform Merge::MultiRowLookup,
          lookup: @loc,
          keycolumn: :normhomeloc,
          fieldmap: {
            newhome: :loc_auth,
            homenote: :map_to_note
          }
        transform Merge::MultiRowLookup,
          lookup: @loc,
          keycolumn: :normloc,
          fieldmap: {
            newloc: :loc_auth,
            locnote: :map_to_note
          }
        # END SECTION

        # SECTION BELOW merges location type and orig value into loc notes
        transform do |row|
          %i[homenote locnote].each do |note|
            note_val = row.fetch(note, nil)
            if note_val.blank?
              row[note] = nil
            else
              orig = note == :homenote ? row.fetch(:home_location) : row.fetch(:location)
              note_val = note_val.sub('origvaluehere', orig)
              label = note == :homenote ? 'HOME LOCATION: ' : 'LOCATION: '
              note_val = "#{label}#{note_val}"
              row[note] = note_val
            end
          end
          row
        end
        # END SECTION

        transform CombineValues::FromFieldsWithDelimiter,
          sources: %i[newhome newloc],
          target: :concat,
          sep: ' ',
          delete_sources: false

        #show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/cat_locs_prep.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          label = 'prelim_locations/obj_prepare: object data cleaned and flagged'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@obj_prepare)
    end

    def self.report_objects_with_unmapped_locations
      Mimsy::Locations.obj_prepare
      
      @unmapped_loc_report = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/cat_locs_prep.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :reject, field: :concat
        #show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports/OBJECTS_with_unmapped_locations.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          label = 'prelim_locations/report_objects_with_unmapped_locations'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@unmapped_loc_report)
    end
    
    def self.obj_locs_clean
      Mimsy::Location.obj_prepare
      
      @obj_locs_clean = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/cat_locs_prep.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldPopulated, action: :keep, field: :concat
        transform Delete::FieldsExcept, keepfields: %i[id_number newhome newloc homenote locnote]
        # show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/object_locations_clean.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          label = 'prelim_locations/obj_locs_clean : Object-location data, cleaned'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(@obj_locs_clean)
    end
  end
end
