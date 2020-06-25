require_relative 'config'

acq_items_no_cat = Kiba.parse do
  @srcrows = 0
  @outrows = 0
  
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldPopulated, action: :reject, field: :m_id
  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/reports_ok/acq_items_no_cat.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQ ITEMS WITH NO CAT"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(acq_items_no_cat)


acq_items_multiple_cat = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe

  @srcrows = 0
  @outrows = 0

  @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/catalogue.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :mkey)

  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldPopulated, action: :keep, field: :m_id
  transform Rename::Field, from: :m_id, to: :mkey

  transform Merge::CountOfMatchingRows,
    lookup: @cat,
    keycolumn: :mkey,
    targetfield: :cat_ct

  transform FilterRows::FieldValueGreaterThan,
    action: :keep,
    field: :cat_ct,
    value: 1

  show_me!

  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/reports/acq_items_multiple_cat.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQ ITEMS WITH MULTIPLE CAT"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(acq_items_multiple_cat)

acq_items_one_cat = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe

  @srcrows = 0
  @outrows = 0

  @cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/catalogue.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :mkey)

  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  transform FilterRows::FieldPopulated, action: :keep, field: :m_id
  transform Rename::Field, from: :m_id, to: :mkey

  transform Merge::CountOfMatchingRows,
    lookup: @cat,
    keycolumn: :mkey,
    targetfield: :cat_ct

  transform FilterRows::FieldEqualTo,
    action: :keep,
    field: :cat_ct,
    value: 1

#  show_me!

  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/reports_ok/acq_items_one_cat.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQ ITEMS WITH ONE CAT"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(acq_items_one_cat)

