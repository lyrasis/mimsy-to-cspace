require_relative 'config'

# produce working table without uncataloged rows
ai_job = Kiba.parse do
  source Kiba::Common::Sources::CSV,
    filename: "#{DATADIR}/mimsy/acquisition_items.tsv",
    csv_options: TSVOPT

  transform { |r| r.to_h }

  transform FilterRows::FieldPopulated, action: :keep, field: :m_id
  transform Rename::Field, from: :m_id, to: :mkey
  filename = "#{DATADIR}/working/acq_items.tsv"
  destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
  
  post_process do
    puts "File generated in #{filename}"
  end
end

Kiba.run(ai_job)

# conversion of Mimsy data to CollectionSpace Acquisition procedure csv
objacqjob = Kiba.parse do
  @ai_cat = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/acq_items.tsv",
                                     csvopt: TSVOPT,
                                     keycolumn: :mkey)
  @acq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/acquisitions.tsv",
                                  csvopt: TSVOPT,
                                  keycolumn: :akey)
  source Kiba::Common::Sources::CSV, filename: "#{DATADIR}/mimsy/catalogue.tsv", csv_options: TSVOPT
  # Ruby's CSV gives us "CSV::Row" but we want Hash
  transform { |r| r.to_h }

  transform Merge::MultiRowLookup,
    lookup: @ai_cat,
    keycolumn: :mkey,
    fieldmap: {:akey => :akey,
               :ai_id_number => :id_number,
               :ai_item_summary => :item_summary,
               :ai_status => :status,
               :ai_status_date => :status_date,
               :ai_accession_date => :accession_date,
               :ai_title_transfer_requested => :title_transfer_requested,
               :ai_transfer_date => :transfer_date,
               # :total_cost ommitted -- not populated
               # :value_currency omitted -- one row has value
               # :item_marked omitted -- all = N
               # :reproduction_request omitted -- all = N
               :ai_note => :note},
    delim: MVDELIM

  transform Merge::MultiRowLookup,
    lookup: @acq,
    keycolumn: :akey,
    fieldmap: {
      :aq_ref_number => :ref_number,
      :aq_status => :status,
      :aq_status_date => :status_date,
      :aq_requested_by => :requested_by,
      :aq_request_date => :request_date,
      :aq_authorized_by => :authorized_by,
      :aq_date_authorized => :date_authorized,
      :aq_method => :method,
      :aq_legal_date => :legal_date,
      :aq_legal_date_display => :legal_date_display,
      :aq_reason => :reason,
      :aq_total_requested => :total_requested,
      :aq_decision_reason => :decision_reason,
      # :total_offer_price ommitted -- only one row has a value
      :aq_terms => :terms,
      :aq_external_file => :external_file
    },
    delim: MVDELIM  
#  extend Kiba::Common::DSLExtensions::ShowMe
#  show_me!

  filename = 'data/working/object-acq.tsv'
  destination Kiba::Extend::Destinations::CSV,
    filename: filename,
    csv_options: TSVOPT,
    initial_headers: [:mkey, :akey, :aq_ref_number, :id_number, :ai_id_number]
  
  post_process do
    puts "File generated in #{filename}"
  end
end

Kiba.run(objacqjob)
