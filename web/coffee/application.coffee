React = require 'react'
ReactDOM = require 'react-dom'
Reactable = require '../coffee/reactable'

class Header extends Reactable
  render: ->
    @div className: "columns",
      @div(className: "three-fifths column",
        @span className: "text-bold", "Name"),
      @div(className: "one-fifth column",
        @span className: "text-bold", "Modified"),
      @div(className: "one-fifth column",
        @span className: "text-bold", "Permissions")

class Row extends Reactable
  render: ->
    @div className: "columns",
      @div(className: "three-fifths column", "#{@props.name}"),
      @div(className: "one-fifth column", "#{@props.modified}"),
      @div(className: "one-fifth column", "#{@props.permissions}")

class FileSystemTable extends Reactable
  render: ->
    @div className: "three-fourths column",
      Header.new(),
      @div(className: "divider"),
      Row.new(name: "folderA", modified: "2017-01-01 00:00:00", permissions: "rwxrwxrwx"),
      Row.new(name: "folderB", modified: "2017-01-01 00:00:00", permissions: "rwxrwxrwx")

class Info extends Reactable
  render: ->
    @div className: "one-fourth column",
      @div className: "blankslate clean-background",
        @p {}, "Select item on the left to view details"

class Application extends Reactable
  render: ->
    @div className: "columns",
      FileSystemTable.new(),
      Info.new()

ReactDOM.render Application.new(), document.getElementById('view')
