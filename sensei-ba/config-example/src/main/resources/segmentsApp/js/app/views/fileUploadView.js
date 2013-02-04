var FileUploadView = Backbone.View.extend({
  el : "#upload-view",
  template : Handlebars.compile($("#fileUpload").html()),

  spinOpts : {
    lines: 15,
    length: 29,
    width: 10,
    radius: 40,
    corners: 1,
    rotate: 73,
    color: '#000',
    speed: 0.8,
    trail: 60,
    shadow: false,
    hwaccel: false,
    className: 'spinner',
    zIndex: 2e9,
    top: 'auto',
    left: 'auto'
  },

  initialize: function() {
    _.bindAll(this, 'render');
    this.render();
    var that = this;
    $( 'form' ).submit(function ( e ) {
        $(that.el).find(".cancel-confirm-overlay").show();
        var target = document.getElementById('upload-file-spinner');
        var spinner = new Spinner(this.spinOpts).spin(target);
        var data, xhr;
        var files = $( '#file' )[0].files;
        data = new FormData();
        data.append( "file", files[0]);
        xhr = new XMLHttpRequest();
        xhr.open( 'POST', '/files', true );
        xhr.onreadystatechange = function ( response ) {
          $(that.el).find(".cancel-confirm-overlay").hide();
          location.reload(true);
        };
        xhr.send(data);
        console.log(data);
        console.log(files);
        e.preventDefault();
    });
  },

  render : function() {
    $(this.el).html(this.template());
  }
});