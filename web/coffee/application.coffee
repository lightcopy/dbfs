React = require 'react'
ReactDOM = require 'react-dom'
Reactable = require '../coffee/reactable'

createView = (spec) ->
  React.createFactory(React.createClass(spec))

class Application extends Reactable
  render: ->
    @div className: "columns",
      @div className: "three-fourths column", "File system"
      @div className: "one-fourth column", "Data"

ReactDOM.render Application.new(), document.getElementById('view')
