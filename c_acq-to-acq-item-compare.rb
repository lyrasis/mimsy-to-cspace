require_relative 'config'

acq_no_acq_items = Kiba.parse do
  @srcrows = 0
  @outrows = 0

  @acqitems = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisition_items.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :akey)
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/acquisitions.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }

  transform{ |r| @srcrows += 1; r }

  transform Merge::CountOfMatchingRows,
    lookup: @acqitems,
    keycolumn: :akey,
    targetfield: :ai_ct

  transform FilterRows::FieldEqualTo, action: :keep, field: :ai_ct, value: 0

  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/working/acq_no_acq_items.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQ WITH NO ACQ ITEMS"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(acq_no_acq_items)

acq_items_no_acq = Kiba.parse do
  @srcrows = 0
  @outrows = 0

  @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisitions.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :akey)
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }

  transform{ |r| @srcrows += 1; r }

  transform Merge::CountOfMatchingRows,
    lookup: @acq,
    keycolumn: :akey,
    targetfield: :a_ct

  transform FilterRows::FieldEqualTo, action: :keep, field: :a_ct, value: 0

  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/working/acq_items_no_acq.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQ ITEMS WITH NO ACQ"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(acq_items_no_acq)

acq_items_one_acq = Kiba.parse do
  @srcrows = 0
  @outrows = 0

  @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisitions.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :akey)
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }

  transform{ |r| @srcrows += 1; r }

  transform Merge::CountOfMatchingRows,
    lookup: @acq,
    keycolumn: :akey,
    targetfield: :a_ct

  transform FilterRows::FieldEqualTo, action: :keep, field: :a_ct, value: 1

  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/working/acq_items_one_acq.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQ ITEMS WITH ONE ACQ"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(acq_items_one_acq)

acq_items_multiple_acq = Kiba.parse do
  @srcrows = 0
  @outrows = 0

  @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisitions.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :akey)
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
    csv_options: TSVOPT

  transform{ |r| r.to_h }

  transform{ |r| @srcrows += 1; r }

  transform Merge::CountOfMatchingRows,
    lookup: @acq,
    keycolumn: :akey,
    targetfield: :a_ct

  transform FilterRows::FieldValueGreaterThan, action: :keep, field: :a_ct, value: 1

  transform{ |r| @outrows += 1; r }
  
  filename = "#{DATADIR}/working/acq_items_multiple_acq.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "\n\nACQ ITEMS WITH MULTIPLE ACQ"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end

Kiba.run(acq_items_multiple_acq)
