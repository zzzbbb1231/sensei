(function(window, document, undefined) {

  var Spinner = function Spinner(o) {
    available = ['spinner_16x16.gif', 'spinner_40x40.gif', 'spinner_75x75.gif'];
    switch(o.radius) {
      case 4: {this.image = available[0]; break;}
      case 11: {this.image = available[1]; break;}
      case 24: {this.image = available[2]; break;}
    }
    this.offset = o.radius + o.width;
  }
  
  Spinner.prototype = {
    spin: function(target) {
      this.el = "<img src='"+eventsAssetUrl+"/images/"+this.image+"' style='position:absolute;margin-left:-"+this.offset+"px;margin-top:-"+this.offset+"px'>";
      return this;
    }
  }
  window.Spinner = Spinner;

})(window, document);
