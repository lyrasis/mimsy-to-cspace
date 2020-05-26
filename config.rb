require 'kiba'
require 'kiba-common/sources/csv'
require 'kiba-common/destinations/csv'
require 'kiba-common/dsl_extensions/show_me'
require 'kiba/extend'
require 'pry'
require 'facets/kernel/blank'

TSVOPT = {headers: true, col_sep: "\t", header_converters: :symbol, converters: [:stripplus]}
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
