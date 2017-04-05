React = require 'react'
ReactDOM = require 'react-dom'
Reactable = require '../coffee/reactable'

class Header extends Reactable
  render: ->
    @div(className: "columns",
      @div(className: "three-fifths column",
        @span(className: "text-bold", "Name")
      ),
      @div(className: "one-fifth column",
        @span(className: "text-bold", "Modified")
      ),
      @div(className: "one-fifth column",
        @span(className: "text-bold", "Permissions")
      )
    )

class Icon extends Reactable
  fsicon: (typeName) ->
    name = typeName?.toLowerCase()
    if name == "directory"
      return "octicon-file-directory"
    if name == "file"
      return "octicon-file-text"
    # otherwise return symlink
    return "octicon-file-symlink-file"

  render: ->
    className = @fsicon(@props.typeName)
    @span(className: "centered mega-octicon #{className}")

# Row takes data {"type": "?", "name": "?", "modified": "?", "permissions": "?"}
class Row extends Reactable
  render: ->
    @div className: "browse-file-row columns",
      @div(className: "three-fifths column",
        @div(className: "columns",
          @div(className: "one-eighth column",
            Icon.new(@props)
          ),
          @div(className: "four-fifths aligned column",
            @div({}, "#{@props.name}")
          )
        )
      ),
      @div(className: "one-fifth column",
        @div({}, "#{@props.modified}")
      ),
      @div(className: "one-fifth column", "#{@props.permissions}")

class FileSystemTable extends Reactable
  render: ->
    @div(className: "three-fourths column",
      Header.new(),
      Row.new(typeName: "directory", name: "FolderA", modified: "2017-01-01 00:00:00", permissions: "rwxrwxrwx"),
      Row.new(typeName: "file", name: "FileA", modified: "2017-01-01 00:00:00", permissions: "rwxrwxrwx"),
      Row.new(typeName: "symlink", name: "SymlinkA", modified: "2017-01-01 00:00:00", permissions: "rwxrwxrwx")
    )

class Info extends Reactable
  render: ->
    @div(className: "one-fourth column",
      @div(className: "blankslate clean-background",
        @div(className: "mega-octicon octicon-eye"),
        @p({}, "Select item on the left to view details")
      )
    )

class Application extends Reactable
  render: ->
    @div className: "columns",
      FileSystemTable.new(),
      Info.new()

# ReactDOM.render Application.new(), document.getElementById('view')
