# frozen_string_literal: true

# prep
#  - prepares notepad table for merge into object records
# report
#  - prepare report of object numbers (not mkeys) and values
module Mimsy
  module Notepad
    extend self

    def prep
      prepjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/notepad.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Delete::Fields, fields: %i[id]
        transform Rename::Field, from: :table_key, to: :mkey

        # remove rows with no note values
        transform CombineValues::FromFieldsWithDelimiter, sources: %i[subject note],
          target: :concat,
          sep: ' ',
          delete_sources: false
        transform FilterRows::FieldPopulated, action: :keep, field: :concat
        transform Delete::Fields, fields: %i[concat]

        # If there is no note, value = subject
        transform do |row|
          subject = row.fetch(:subject)
          note = row.fetch(:note)
          value = note.blank? ? subject : note
          row[:value] = value
          row
        end

        transform Clean::RegexpFindReplaceFieldVals,
          fields: %i[value],
          find: ';',
          replace: ', '

        transform Delete::FieldsExcept, keepfields: %i[mkey value]
        #  show_me!

        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/working/notepad.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[mkey],
          csv_options: TSVOPT
        label = 'Notes to merge'
        
        post_process do
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(prepjob)
    end

    def report
      prep unless File.file?("#{DATADIR}/working/notepad.tsv")
      Mimsy::Cat.setup unless File.file?("#{DATADIR}/working/catalogue.tsv")
      
      repjob = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/catalogue.tsv",
                                          csvopt: TSVOPT,
                                          keycolumn: :mkey)


        source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/notepad.tsv", csv_options: TSVOPT
        transform { |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Merge::MultiRowLookup,
          lookup: @cat,
          keycolumn: :mkey,
          fieldmap: {
            :objectnumber => :id_number,
          },
          delim: MVDELIM

        transform Delete::Fields, fields: %i[mkey]

        transform FilterRows::FieldPopulated, action: :keep, field: :objectnumber
        #  show_me!

        
        transform{ |r| @outrows += 1; r }
        filename = "#{DATADIR}/reports_ok/notepad_notes.tsv"
        destination Kiba::Extend::Destinations::CSV,
          filename: filename,
          initial_headers: %i[objectnumber],
          csv_options: TSVOPT
        label = 'Notes from notepad'
        
        post_process do
          puts "\n\n#{label.upcase}"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(repjob)
    end
  end
end
