var SegmentsMamangementView = Backbone.View.extend({
  el : "#segments-management-view",
  template : Handlebars.compile($("#segmentsMamangement").html()),

  initialize : function(params) {
    _.bindAll(this, 'render');
    this.collection.bind('reset', this.render);
    this.render();
  },

  render : function() {
    console.log("render");
    $(this.el).html(this.template());
    var that = this;
    _.each(this.collection.models, function(ele, index) {
      var view = new SegmentItemView({model:ele});
      $(that.el).find("#segments-list-view").append(view.render().el);
    });
  }
});