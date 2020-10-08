# frozen_string_literal: true

# with_variants
#  - merge subject_variations terms into subjects
#  - create basic CSpace concept field structure
#  - normalize and populate duplicates
# duplicates
#  - report of duplicate terms -- match in Cspace will be on string, so we'll only be importing
#    one record for each string. Deduplication is done on normalized values (normalization
#    rules used to create string-based ID in CSpace)
# unlinked_variants
#  - reports any subject_variations without matches to subjects
# n/a -- merge_var_orphans -- add results from above to list of subjects
# extract_broader_terms
#  - create working table of broader terms, with CSpace fields. Lookup from merge_var result to
#    identify and remove any that are already in the subject list
# unique_subjects
#  - extracts only the unique subjects
# item_subject_lookup
#  - creates lookup table matching string associated with object/item (which may vary slightly in
#    form) with the form used to create Concept authority
# all
#  - combine main subject list with broader subject list
#  - deduplicate again

module Mimsy
  module Subject
    extend self

    def with_variants
      merge_var = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}

        @vars = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/subject_variations.tsv",
                                         csvopt: TSVOPT,
                                         keycolumn: :subkey)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/subjects.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Rename::Field, from: :subject, to: :termDisplayName
        # termSourceNote because these often say "this is official LOC heading" or other note
        #   about actual source/form of term
        transform Rename::Field, from: :note, to: :termSourceNote
        transform Rename::Field, from: :description, to: :scopeNote
        transform Merge::ConstantValue, target: :termPrefForLang, value: 'true'
        transform Merge::ConstantValue, target: :termSourceLocal, value: 'Mimsy export'
        transform Merge::ConstantValue, target: :termSourceDetail, value: 'subjects.csv/SUBJECT'
        transform Replace::FieldValueWithStaticMapping, source: :language, target: :termLanguage, mapping: LANGUAGES

        #SECTION below retains only the first broader term and normalizes it for creation of hierarchy later
        
        transform Rename::Field, from: :subject_category, to: :broaderTerm
        transform do |row|
          bt = row.fetch(:broaderTerm, nil)
          if bt.nil? || bt.empty?
            row[:broaderTerm] = nil
          else
            row[:broaderTerm] = bt.split(MVDELIM).first
          end
          row
        end
        transform Cspace::NormalizeForID, source: :broaderTerm, target: :broaderNorm
        #END SECTION

        transform Merge::MultiRowLookup,
          lookup: @vars,
          keycolumn: :msub_id,
          fieldmap: {:termDisplayNameNonPreferred => :variation},
          constantmap: {
            :termPrefForLangNonPreferred => 'false',
            :termSourceLocalNonPreferred => 'Mimsy export',
            :termSourceDetailNonPreferred => 'subject_variations.csv/VARIATION'
          },
          conditions: {
            exclude: {
              field_equal: { fieldsets: [
                {
                  type: :any,
                  matches: [
                    ['row::termDisplayName', 'mergerow::variation']
                  ]
                }
              ]}
            }
          },
          delim: MVDELIM

        transform Clean::StripFields, fields: %i[termDisplayName termDisplayNameNonPreferred]

        #SECTION below handles creating a normalized preferred term and deduplicating on it
        transform Cspace::NormalizeForID, source: :termDisplayName, target: :termNorm
        transform Deduplicate::Flag, on_field: :termNorm, in_field: :duplicate, using: @deduper
        #END SECTION
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/subjects_with_vars.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nDUPE-FLAGGED SUBJECTS WITH VARIATIONS MERGED IN"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(merge_var)
    end

    def duplicates
      with_variants unless File.file?("#{DATADIR}/working/subjects_with_vars.tsv")
      
      dupe_report = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/subjects_with_vars.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'y'
        #show_me!
        
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports/DUPLICATE_subjects.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nDUPLICATE SUBJECTS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(dupe_report)
    end

    def unlinked_variants
      var_report = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}

        @subs = Lookup.csv_to_multi_hash(file: "#{DATADIR}/mimsy/subjects.tsv",
                                         csvopt: TSVOPT,
                                         keycolumn: :msub_id)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/subject_variations.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Merge::MultiRowLookup,
          lookup: @subs,
          keycolumn: :subkey,
          fieldmap: {:termDisplayName => :subject},
          delim: MVDELIM

        transform FilterRows::FieldPopulated, action: :reject, field: :termDisplayName
        
        #show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/reports/variant_subjects_with_no_prefTerm.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nVARIANT SUBJECTS WITH NO PREFERRED TERM"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(var_report)
    end

    def extract_broader_terms
      with_variants unless File.file?("#{DATADIR}/working/subjects_with_vars.tsv")
      extract_broader = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}

        @subs = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/subjects_with_vars.tsv",
                                         csvopt: TSVOPT,
                                         keycolumn: :termnorm)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/subjects.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Delete::FieldsExcept, keepfields: %i[subject_category]
        transform Explode::RowsFromMultivalField, field: :subject_category, delim: MVDELIM
        transform Clean::StripFields, fields: %i[subject_category]

        #SECTION below handles creating a normalized preferred term and deduplicating on it
        transform Cspace::NormalizeForID, source: :subject_category, target: :broadNorm
        transform Deduplicate::Flag, on_field: :broadNorm, in_field: :duplicate, using: @deduper
        #END SECTION

        transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'

        # SECTION below brings in a column from working subject list if broader term is already there,
        #   then keeps only broader terms not listed there already
        transform Merge::MultiRowLookup,
          lookup: @subs,
          keycolumn: :broadNorm,
          fieldmap: {:termDisplayName => :subject},
          delim: MVDELIM
        transform FilterRows::FieldPopulated, action: :reject, field: :termDisplayName
        transform Delete::Fields, fields: %i[termDisplayName duplicate]
        #END SECTION

        transform Rename::Field, from: :subject_category, to: :termDisplayName
        transform Rename::Field, from: :broadNorm, to: :termNorm
        transform Merge::ConstantValue, target: :termLanguage, value: 'English'
        transform Merge::ConstantValue, target: :termPrefForLang, value: 'true'
        transform Merge::ConstantValue, target: :termSourceLocal, value: 'Mimsy export'
        transform Merge::ConstantValue, target: :termSourceDetail, value: 'subjects.csv/SUBJECT_CATEGORY'

        #SECTION below makes this table match the one we're going to append it to
        transform do |row|
          nilfields = %i[msub_id termSourceNote scopeNote termDisplayNameNonPreferred termPrefForLangNonPreferred termSourceLocalNonPreferred termSourceDetailNonPreferred broaderterm broadernorm]
          nilfields.each{ |nf| row[nf] = nil }
          row[:duplicate] = 'n'
          row
        end
        #END SECTION

        transform FilterRows::FieldPopulated, action: :keep, field: :termDisplayName
        #show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/subjects_broader.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nBROADER SUBJECTS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(extract_broader)
    end

    def unique_subjects
      with_variants unless File.file?("#{DATADIR}/working/subjects_with_vars.tsv")
      subs_deduped = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/subjects_with_vars.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform FilterRows::FieldEqualTo, action: :keep, field: :duplicate, value: 'n'
        #show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/subjects_deduped.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nDEDUPLICATED SUBJECTS"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(subs_deduped)
    end

    def item_subject_lookup
      unique_subjects unless File.file?("#{DATADIR}/working/subjects_deduped.tsv")
      
      create_co_lookup = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0

        @subsall = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/subjects_with_vars.tsv",
                                            csvopt: TSVOPT,
                                            keycolumn: :msub_id)
        @subsuniq = Lookup.csv_to_multi_hash(file: "#{DATADIR}/working/subjects_deduped.tsv",
                                             csvopt: TSVOPT,
                                             keycolumn: :termnorm)

        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/mimsy/item_subjects.tsv",
          csv_options: TSVOPT

        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        #show_me!
        transform Merge::MultiRowLookup,
          lookup: @subsall,
          keycolumn: :subkey,
          fieldmap: {:norm => :termnorm}

        transform Merge::MultiRowLookup,
          lookup: @subsuniq,
          keycolumn: :norm,
          fieldmap: {:migratingsub => :termdisplayname}

        transform Delete::Fields, fields: %i[subkey subject norm]
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/subject_item_lookup.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nITEM-SUBJECT LOOKUP"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(create_co_lookup)
    end

    def all
      unique_subjects unless File.file?("#{DATADIR}/working/subjects_deduped.tsv")
      extract_broader_terms unless File.file?("#{DATADIR}/working/subjects_broader.tsv")

      all_subjects = Kiba.parse do
        extend Kiba::Common::DSLExtensions::ShowMe
        @srcrows = 0
        @outrows = 0
        @deduper = {}
        
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/subjects_deduped.tsv",
          csv_options: TSVOPT
        source Kiba::Common::Sources::CSV,
          filename: "#{DATADIR}/working/subjects_broader.tsv",
          csv_options: TSVOPT
        
        transform{ |r| r.to_h }
        transform{ |r| @srcrows += 1; r }

        transform Delete::Fields, fields: %i[duplicate]
        transform Deduplicate::Flag, on_field: :termnorm, in_field: :duplicate, using: @deduper
        transform FilterRows::FieldEqualTo, action: :reject, field: :duplicate, value: 'y'

        #show_me!
        transform{ |r| @outrows += 1; r }
        
        filename = "#{DATADIR}/working/subjects_all.tsv"
        destination Kiba::Extend::Destinations::CSV, filename: filename, csv_options: TSVOPT
        
        post_process do
          puts "\n\nSUBJECTS COMBINED"
          puts "#{@outrows} (of #{@srcrows})"
          puts "file: #{filename}"
        end
      end
      Kiba.run(all_subjects)
    end
  end
end
