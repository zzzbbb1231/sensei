var SegmentsCollection = Backbone.Collection.extend({
  model: Segment,
  baseUrl: "/segments",
  currentPartition : null,
  allPartitions : null,

  url: function() {
    if (this.currentPartition && this.currentPartition != "all") {
      return "/segments/" + this.currentPartition;
    } else {
      return this.baseUrl;
    }
  },

  parse: function(response) {
    var stats = null;
    if (this.currentPartition && this.currentPartition != "all") {
      stats = parseSegmentLevelStats(response);
      var models = [];
      for (var i = 0 ; i < stats.segments.length; i++) {
        stats.segments[i].partitionName = this.currentPartition;
      }
    } else {
      stats = parseStats(response);
      this.allPartitions = stats.partitions;
    }

    return stats.segments;
  }

});