var Segment = Backbone.Model.extend({
  initialize : function() {
    this.set({'indexVersion' : this.get('index.version')});
    if (this.get('segment.Type')) {
      this.set({'timeType' : this.get('segment.Type')});
    } else {
      this.set({'timeType' : this.get('segment.time.Type')});
    }

    this.set({"aggregation" : this.get("segment.aggregation")});
    this.set({"clusterName" : this.get("segment.cluster.name")});
    if (this.get('segment.startTime')) {
      var startTime = parseFloat(this.get('segment.startTime'));
      this.set({'startTime' : startTime});
    }

    if (this.get('segment.endTime')) {
      var endTime = parseFloat(this.get('segment.endTime'));
      this.set({'endTime' : endTime});
    }

  },

  promptColor: function() {
    var cssColor = prompt("Please enter a CSS color:");
    this.set({color: cssColor});
  }
});