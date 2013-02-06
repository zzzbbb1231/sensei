var State = {
  level1Left: false,
  level2Left : false,
  level1Right: false,
  level2Right: true
};

var regionMap = {
  "1063":"Cisco",
  "1028":"Oracle",
  "1441":"Google",
  "1035":"Microsoft",
  "1025":"HP",
  "1288":"Yahoo",
  "1038":"Deloitte",
  "1009":"IBM",
  "1283":"Infosys",
  "1337":"LinkedIn"
};

var URL = {
  base: "http://eat1-app91.stg.linkedin.com:10295/tapservice/resources/stats?q=stats&source=csapEvents",
  baseFilter: "filter=accountId:1000:EQ",
  baseAggregate: "views:sum",
  baseGroupBy: "groupBy=source:10"
};

var level1Filter = {
  left : null,
  right : null
};

var Type = {
  type : null
};

var DrawLogic = {
  drawGraphFor: function(level, data, side) {
    var url = "";
    if (Type.type == 1) {
      if (level == 1) {
        url = URL.base + "&filter=" + URL.baseFilter + ";source:" + data + ":EQ&aggregate=" + URL.baseAggregate + "&groupBy=memberRegion:10";
      } else {
        if (side == "L") {
          url = URL.base + "&filter=" + URL.baseFilter + ";jobRegion:" + data + ":EQ;source:" + level1Filter.left +":EQ&aggregate=" + URL.baseAggregate + "&groupBy=memberFunction:10";
        } else {
          url = URL.base + "&filter=" + URL.baseFilter + ";jobRegion:" + data + ":EQ;source:" + level1Filter.right +":EQ&aggregate=" + URL.baseAggregate + "&groupBy=memberFunction:10";
        }
      }
    } else {
      if (level == 1) {
        url = URL.base + "&filter=" + URL.baseFilter + ";memberCompany:" + data + ":EQ&aggregate=" + URL.baseAggregate + "&groupBy=memberFunction:10";
      } else {
        if (side == "L") {
          url = URL.base + "&filter=" + URL.baseFilter + ";memberFunction:" + data + ":EQ;memberCompany:" + level1Filter.left +":EQ&aggregate=" + URL.baseAggregate + "&groupBy=memberRegion:10";
        } else {
          url = URL.base + "&filter=" + URL.baseFilter + ";memberFunction:" + data + ":EQ;memberCompany:" + level1Filter.right +":EQ&aggregate=" + URL.baseAggregate + "&groupBy=memberRegion:10";
        }
      }
    }
    console.log(url);
      $.ajax({
        url: "/home/get_from_pinot?url=" + escape(url),
        beforeSend: function ( xhr ) {
          xhr.overrideMimeType("text/plain; charset=x-user-defined");
        }
      }).done(function ( d ) {
        var jsonData = JSON.parse(d);
        if (level == 1) {
          if (side == "L") {
            $("#level1-left-graph").html("");
            Graphs.drawFirstGraph(jsonData, 'level1-left-graph', 1, "L");
          } else {
            $("#level1-right-graph").html("");
            Graphs.drawFirstGraph(jsonData, 'level1-right-graph', 1, "R");
          }
        } else {
          if (side == "L") {
            $("#level2-left-graph").html("");
            Graphs.drawFirstGraph(jsonData, 'level2-left-graph', 2, "L");
          } else {
            $("#level2-right-graph").html("");
            Graphs.drawFirstGraph(jsonData, 'level2-right-graph', 2, "R");
          }
        }
      });
    }
}