var SegmentItemView = Backbone.View.extend({
  template : Handlebars.compile($("#segment").html()),

  events : {
    'click .delete' : "deleteConfirm",
    'click .move' : "moveConfirm",
    'click .cancel-confirm-overlay' : "hideOverlay",
    'click .cancel-button'  : "hideOverlay",
    'click .delete-button' : "deleteSegmentForReals",
    'click .move-button' : "moveSegmentsForReals"
  },

  render : function() {
    this.model.set({"partitions" : this.filterPartitions()});
    $(this.el).html(this.template(this.model.toJSON()));
    this.collection = this.model.collection;
    return this;
  },

  filterPartitions : function() {
    var partions = [];
    var that = this;
    for (var i = 0 ; i < this.model.collection.allPartitions.length; i++) {
      if (this.model.collection.allPartitions[i].toString() !== this.model.get("partitionName").toString()) {
        partions.push(this.model.collection.allPartitions[i].toString());
      }
    }
    return partions;
  },

  moveConfirm : function() {
    console.log("move");
    $(this.el).find(".cancel-confirm-overlay").show();
    $(this.el).find(".confirm-move-box").show();
    $(this.el).find(".confim-dialog-box").hide();
  },

  deleteConfirm : function() {
    $(this.el).find(".confirm-move-box").hide();
    $(this.el).find(".cancel-confirm-overlay").show();
    $(this.el).find(".confim-dialog-box").show();
  },

  deleteSegmentForReals: function() {
    var that = this;
    var deleteUrl = "/" +this.model.get("partitionName") + "/" + this.model.get("segmentId") + "?delete";
    $.ajax({
      url : "/segments" + deleteUrl,
      beforeSend : function(xhr) {
        xhr.overrideMimeType("text/plain; charset=x-user-defined");
      }
    }).done(function(data) {
      that.collection.fetch({add:false});
    });
  },

  moveSegmentsForReals: function() {
    var that = this;
    var selectedPartion = $(this.el).find("#partions-select-for-move-drop-down").val();
    var moveUrl = "/" +this.model.get("partitionName") + "/" + this.model.get("segmentId") + "?move=" + selectedPartion;
    $.ajax({
      url : "/segments" + moveUrl,
      beforeSend : function(xhr) {
        xhr.overrideMimeType("text/plain; charset=x-user-defined");
      }
    }).done(function(data) {
      that.collection.fetch({add:false});
    });
  },

  hideOverlay: function() {
    $(this.el).find(".cancel-confirm-overlay").hide();
    $(this.el).find(".confim-dialog-box").hide();
    $(this.el).find(".confirm-move-box").hide();
  }
});
