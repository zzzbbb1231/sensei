var PartitionsView = Backbone.View.extend({
  template : Handlebars.compile($("#partitions").html()),

  initialize : function(params) {
    this.el = "#partitions-view";
    this.data = params.data;
    this.collection = params.segmentsCollection;
    _.bindAll(this, 'render', 'updateSegmentsView', 'applyCustomFilter');
    this.render();
    var that = this;
    $(this.el).find(".filter-button").click(function() {
      that.applyCustomFilter();
    });
    
    $(this.el).find("#predefined-action-today").click(function() {
      that.applyTodayFilter();
    });
    
    $(this.el).find("#predefined-action-yesterday").click(function() {
      that.applyYesterdayFilter();
    });
    
    $(this.el).find("#predefined-action-this-week").click(function() {
      that.applyWeekFilter();
    });
    
    $(this.el).find("#predefined-action-this-month").click(function() {
      that.applyMonthFilter();
    });
    
    $(this.el).find("#custom-time-from").datepicker();
    $(this.el).find("#custom-time-to").datepicker();
    $('select').change(function() {
      that.updateSegmentsView(this);
    });
  },

  render : function() {
    $(this.el).html(this.template(this.data));
    return this;  
  },

  updateSegmentsView : function(info) {
    var currentSelection = $(this.el).find("#partions-select-drop-down").val().split("-")[1];
    this.collection.currentPartition = currentSelection;
    this.collection.fetch({add:false});
  },

  applyCustomFilter: function() {
    console.log("custom");
    var startDate = $(this.el).find("#custom-time-from").datepicker( "getDate" );
    var endDate = $(this.el).find("#custom-time-to").datepicker( "getDate" );
    console.log(startDate);
    console.log(endDate);
  },

  applyTodayFilter: function() {
    console.log("today");
    var today = new Date();
    console.log(today);
  },

  applyYesterdayFilter: function() {
    console.log("yesterday");
    var today = new Date();
    var yesterday = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 1);
    console.log(yesterday);
  },

  applyWeekFilter: function() {
    console.log("week");
    var today = new Date();
    var date = today.getDate();
    var day = today.getDay();
    var floor = date - day;
    var ceiling = date + (6 - day);
    var startDay = new Date(today.getFullYear(), today.getMonth(), floor);
    var endDay = new Date(today.getFullYear(), today.getMonth(), ceiling);
    console.log(startDay);
    console.log(endDay);
  },

  applyMonthFilter: function() {
    console.log("month");
    var today = new Date();
    var startDay = new Date(today.getFullYear(), today.getMonth(), 1);
    console.log(startDay);
    console.log(today);
  }

});
