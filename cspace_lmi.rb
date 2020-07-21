require_relative 'config'
require_relative 'prelim_cat'

Mimsy::Cat.setup

# create cspace collectionobject records
lmijob = Kiba.parse do
  extend Kiba::Common::DSLExtensions::ShowMe
  @deduper = {}
  @srcrows = 0
  @outrows = 0


  @locmap = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/location_mapper_no_comma.tsv",
                                       csvopt: TSVOPT,
                                       keycolumn: :norm_value)
  
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/working/catalogue.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }
  transform{ |r| @srcrows += 1; r }

  #  transform FilterRows::FieldEqualTo, action: :keep, field: :mkey, value: '1113'

  # keep only rows with location data
  transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[home_location location],
    target: :concat_loc,
    sep: ' ',
    delete_sources: false
  transform FilterRows::FieldPopulated, action: :keep, field: :concat_loc

  
  # id_number is required
  transform FilterRows::FieldPopulated, action: :keep, field: :id_number

  # SECTION BELOW creates norm_value_homeloc and norm_value_loc
  transform do |row|
    homeloc = row.fetch(:home_location, nil)
    if homeloc.nil? || homeloc.empty?
      row[:norm_homeloc] = nil
    else
      row[:norm_homeloc] = homeloc.strip.gsub(/  +/, ' ').gsub(',', '').downcase
    end
    row
  end
  transform do |row|
    loc = row.fetch(:location, nil)
    if loc.nil? || loc.empty?
      row[:norm_loc] = nil
    else
      row[:norm_loc] = loc.strip.gsub(/  +/, ' ').gsub(',', '').downcase
    end
    row
  end
  # END SECTION

   transform Merge::MultiRowLookup,
    lookup: @locmap,
    keycolumn: :norm_homeloc,
    fieldmap: {
      :auth_homeloc => :loc_auth,
    }
   transform Merge::MultiRowLookup,
    lookup: @locmap,
    keycolumn: :norm_loc,
    fieldmap: {
      :auth_loc => :loc_auth,
    }

  transform CombineValues::FromFieldsWithDelimiter,
    sources: %i[auth_homeloc auth_loc],
    target: :concat_auth,
    sep: ' ',
    delete_sources: false
  transform FilterRows::FieldPopulated, action: :keep, field: :concat_auth


  transform Rename::Field, from: :id_number, to: :objectNumber
  transform Deduplicate::Flag, on_field: :objectNumber, in_field: :duplicate, using: @deduper
  transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
  transform Delete::FieldsExcept, keepfields: %i[objectNumber home_location location norm_homeloc norm_loc auth_loc auth_homeloc]

#  show_me!
  
  transform{ |r| @outrows += 1; r }
  filename = "#{DATADIR}/cs/lmi.csv"
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
   initial_headers: %i[objectNumber],
    csv_options: CSVOPT
    
  post_process do
    puts "\n\nLMI"
    puts "#{@outrows} (of #{@srcrows})"
    puts "file: #{filename}"
  end
end
Kiba.run(lmijob)
