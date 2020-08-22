require 'kiba'
require 'kiba-common/sources/csv'
require 'kiba-common/destinations/csv'
require 'kiba-common/dsl_extensions/show_me'
require 'kiba/extend'
require 'pry'
require 'facets/kernel/blank'

require_relative 'prelim_acqitems'
require_relative 'prelim_cat'
require_relative 'prelim_concept'
require_relative 'prelim_condition'
require_relative 'prelim_inscription'
require_relative 'prelim_locations'
require_relative 'prelim_measurement_prepare'
require_relative 'prelim_names_for_co'
require_relative 'prelim_people_build'
require_relative 'prelim_place'

TSVOPT = {headers: true, col_sep: "\t", header_converters: :symbol, converters: [:stripplus]}
LOCCSVOPT = {headers: true, header_converters: :symbol, converters: [:stripplus]}
MVDELIM = ';'
DATADIR = File.expand_path('~/code/mimsy-to-cspace/data')
LANGUAGES = {
  'eng' => 'English'
}
GENDER = {
  'F' => 'female',
  'M' => 'male',
  'N' => nil
}
INVSTATUS = {
  'ACCESSION' => 'unprocessed',
  'ACCESSION DETAIL' => 'processed'
}
PRIORATTR = {
  'N' => '',
  'Y' => ' (prior attribution)'
}
PUBLISH = {
  'Y' => 'All',
  'N' => 'None'
}
REPRO = {
  'Y' => 'Reproduction allowed.',
  'N' => 'Reproduction not allowed.'
}
MEDIATYPE = {
  'IMAGE' => 'still_image',
  'DOCUMENT' => 'document'
}
