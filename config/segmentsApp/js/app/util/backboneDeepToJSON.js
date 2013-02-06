_.extend(Backbone.Model.prototype, {
  // Version of toJSON that traverses nested models
  deepToJSON: function() {
    var obj = this.toJSON();
    _.each(_.keys(obj), function(key) {
      if (obj[key] && _.isFunction(obj[key].deepToJSON)) {
        obj[key] = obj[key].deepToJSON();
      }
    });
    return obj;
  }
});

_.extend(Backbone.Collection.prototype, {
  // Version of toJSON that traverses nested models
  deepToJSON: function() {
    return this.map(function(model){ return model.deepToJSON(); });
  }
});
