require_relative 'config'
require_relative 'prelim_subject'

Mimsy::Subject.setup

all_subjects = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @srcrows = 0
  @outrows = 0

  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/working/subjects_with_vars.tsv",
    csv_options: TSVOPT
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/working/subjects_broader.tsv",
    csv_options: TSVOPT
  transform{ |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform Delete::Fields, fields: %i[msub_id broaderterm broadernorm termnorm duplicate]
  transform{ |r| @outrows += 1; r }

  filename = "#{DATADIR}/cs/subjects.csv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: CSVOPT
  
  post_process do
    puts "\n\nALL SUBJECTS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(all_subjects)
