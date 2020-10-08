# frozen_string_literal: true

# csv
#  - create movement procedures CSV for CollectionSpace import
# relationship_to_object
#  - create relationships between movement and object records for import

module Cspace
  module Movement
    extend self

    def csv
      Mimsy::Location.finalize_for_mapping unless File.file?("#{DATADIR}/working/object_locations_clean.tsv")
      
    lmijob = Kiba.parse do
      extend Kiba::Common::DSLExtensions::ShowMe
      @deduper = {}
      @srcrows = 0
      @outrows = 0

      source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/object_locations_clean.tsv", csv_options: TSVOPT
      transform { |r| r.to_h }
      transform{ |r| @srcrows += 1; r }

      # SECTION BELOW carries out the following mappings as specified
      # B : Both locations populated, values the same in both
      # C : HOME_LOCATION populated, LOCATION blank
      # D : Both locations populated, values different
      # E : LOCATION populated, HOME_LOCATION blank

      # B - the location is mapped to both "Temp location/building" and "Home location/building" fields
      # C - HOME_LOCATION is mapped to both "Temp location/building" and "Home location/building" fields
      # D - LOCATION is mapped to "Temp location/building" and HOME_LOCATION is mapped to "Home location/building" fields
      # E - LOCATION is mapped to "Temp location/building" and "Home location/building" field is left blank
      #     (on assumption that such items may not have a permanent location assigned)

      # create CS fields so all rows will have both and we don't have to fiddle around with else statements below
      transform do |row|
        row[:normallocationstorage] = nil
        row[:currentlocationstorage] = nil
        row
      end

      # handle B -- IKF 126.002, 00-002 V/F M
      transform do |row|
        nh = row.fetch(:newhome, nil)
        nl = row.fetch(:newloc, nil)
        same = true if nh == nl unless nh.blank?

        if same
          row[:normallocationstorage] = nh
          row[:currentlocationstorage] = nl
        end
        row
      end

      # handle C -- 08-040.T01a-d, 2018.027 O/F B
      transform do |row|
        nh = row.fetch(:newhome, nil)
        nl = row.fetch(:newloc, nil)
        keep = true if !nh.blank? && nl.blank?
        if keep
          row[:normallocationstorage] = nh
          row[:currentlocationstorage] = nh
        end
        row
      end

      # handle D -- MCG 175.089, 89-012.01
      transform do |row|
        nh = row.fetch(:newhome, nil)
        nl = row.fetch(:newloc, nil)
        keep = true
        keep = false if nh.nil?
        keep = false if nl.nil?
        keep = false if nh == nl
        if keep
          row[:normallocationstorage] = nh
          row[:currentlocationstorage] = nl
        end
        row
      end

      # handle E -- 97-079.01, BHF 245.013
      transform do |row|
        nh = row.fetch(:newhome, nil)
        nl = row.fetch(:newloc, nil)
        keep = true
        keep = false if nl.nil?
        keep = false unless nh.nil?
        if keep
          row[:currentlocationstorage] = nl
        end
        row
      end
      # END SECTION

      transform CombineValues::FromFieldsWithDelimiter,
        sources: %i[homenote locnote],
        target: :currentlocationstoragenote,
        sep: '; '

      transform Clean::RegexpFindReplaceFieldVals,
        fields: %i[currentlocationstoragenote],
        find: 'LINEBREAKWASHERE',
        replace: "\n"


      transform Rename::Field, from: :id_number, to: :movementreferencenumber
      transform Prepend::ToFieldValue, field: :movementreferencenumber, value: 'LOC'
      transform Delete::FieldsExcept, keepfields: %i[movementreferencenumber normallocationstorage currentlocationstorage currentlocationstoragenote]

      #show_me!
      
      transform{ |r| @outrows += 1; r }
      filename = "#{DATADIR}/cs/movement.csv"
      destination Kiba::Extend::Destinations::CSV,
        filename: filename,
        initial_headers: %i[movementreferencenumber],
        csv_options: CSVOPT
      
      post_process do
        label = 'CS Movement records'
        puts "\n\n#{label.upcase}"
        puts "#{@outrows} (of #{@srcrows})"
        puts "file: #{filename}"
      end
    end
    Kiba.run(lmijob)
    end
    
    def relationship_to_object
      csv unless File.file?("#{DATADIR}/cs/movement.csv")
      
      lmirels = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @deduper = {}
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/cs/movement.csv", csv_options: LOCCSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Delete::FieldsExcept, keepfields: %i[movementreferencenumber]
        transform Rename::Field, from: :movementreferencenumber, to: :subjectIdentifier
        transform Copy::Field, from: :subjectIdentifier, to: :objectIdentifier
        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[objectIdentifier],
          find: '^LOC',
          replace: ''
        transform Merge::ConstantValue, target: :objectDocumentType, value: 'CollectionObject'
        transform Merge::ConstantValue, target: :subjectDocumentType, value: 'Movement'

        #show_me!
        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/cs/rels_movement-co.csv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[subjectIdentifier subjectDocumentType objectIdentifier],
          csv_options: CSVOPT
        
        post_process do
          label = 'CS Relationship Records: CollectionObject <-> Movement'
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(lmirels)
    end
  end
end
