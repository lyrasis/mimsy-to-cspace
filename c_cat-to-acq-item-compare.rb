require_relative 'config'
require_relative 'prelim_cat_remove_loans'

Mimsy::Cat.setup

cat_no_acq_items = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  
  @srcrows = 0
  @outrows = 0

  @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :m_id)
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/working/catalogue.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }

  transform{ |r| @srcrows += 1; r }

  transform Merge::CountOfMatchingRows,
    lookup: @acqitems,
    keycolumn: :mkey,
    targetfield: :ai_ct

  transform FilterRows::FieldEqualTo, action: :keep, field: :ai_ct, value: 0

#  show_me!
  
  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/reports/cat_no_acq_items.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nCAT WITH NO ACQ ITEMS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(cat_no_acq_items)

cat_one_acq_items = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  
  @srcrows = 0
  @outrows = 0

  @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :m_id)
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/working/catalogue.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }

  transform{ |r| @srcrows += 1; r }

  transform Merge::CountOfMatchingRows,
    lookup: @acqitems,
    keycolumn: :mkey,
    targetfield: :ai_ct

  transform FilterRows::FieldEqualTo, action: :keep, field: :ai_ct, value: 1

#  show_me!
  
  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/reports_ok/cat_one_acq_items.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nCAT WITH ONE ACQ ITEMS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(cat_one_acq_items)

cat_multiple_acq_items = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  
  @srcrows = 0
  @outrows = 0

  @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :m_id)
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/working/catalogue.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }

  transform{ |r| @srcrows += 1; r }

  transform Merge::CountOfMatchingRows,
    lookup: @acqitems,
    keycolumn: :mkey,
    targetfield: :ai_ct

  transform FilterRows::FieldValueGreaterThan, action: :keep, field: :ai_ct, value: 1
  
#  show_me!
  
  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/reports/cat_multiple_acq_items.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nCAT WITH MULTIPLE ACQ ITEMS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(cat_multiple_acq_items)
