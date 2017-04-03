React = require 'react'

# React base class for CoffeeScript
class Reactable extends React.Component
  constructor: (props) ->
    super props

  render: ->

  a: (opts...) -> React.DOM.a(opts...)

  div: (opts...) -> React.DOM.div(opts...)

  h1: (opts...) -> React.DOM.h1(opts...)

  h2: (opts...) -> React.DOM.h2(opts...)

  h3: (opts...) -> React.DOM.h3(opts...)

  input: (opts...) -> React.DOM.input(opts...)

  li: (opts...) -> React.DOM.li(opts...)

  nav: (opts...) -> React.DOM.nav(opts...)

  p: (opts...) -> React.DOM.p(opts...)

  pre: (opts...) -> React.DOM.pre(opts...)

  small: (opts...) -> React.DOM.small(opts...)

  span: (opts...) -> React.DOM.span(opts...)

  textarea: (opts...) -> React.DOM.textarea(opts...)

  ul: (opts...) -> React.DOM.ul(opts...)

  @new: (props) ->
    @factory ?= React.createFactory(@)
    @factory(props)

module.exports = Reactable
