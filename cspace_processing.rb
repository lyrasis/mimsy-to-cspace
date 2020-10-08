# frozen_string_literal: true

require_relative 'config'

## authorities
Cspace::Place.csv
Cspace::Person.csv
Cspace::Organization.csv
Cspace::Location.csv
Cspace::Concept.csv
Cspace::Work.csv

## authority hierarchies
Cspace::Concept.hierarchies
Cspace::Location.hierarchies

## object and object relationships
Cspace::CollectionObject.csv
Cspace::CollectionObject.hierarchy
Cspace::CollectionObject.related

## procedures
Cspace::Acquisition.csv
Cspace::Condition.csv
Cspace::Movement.csv
Cspace::Media.csv

## relationships
Cspace::Acquisition.relationship_to_object
Cspace::Acquisition.relationship_to_acqitem_object
Cspace::Condition.relationship_to_object
Cspace::Movement.relationship_to_object
Cspace::Media.relationship_to_object
