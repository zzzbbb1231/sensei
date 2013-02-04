/*


*/

var utils = {
   type: null,
   columnNames : null,
   rowVals : null,

   init: function(t, colNames, rVals) {
      this.type = t;
      t.columnNames = colNames;
      t.rowVals = rVals;
   }   
};

var Graphs = {
   resetGraphs: function() {
      $("#graph1").hide();
      $("#graph2").hide();
      $("#graph3").hide();
   },

   drawFirstGraph: function(d, id, level, clicker) {
      var data = d.elements[0];
      var hadToSplit = false;
      for (var i=0; i < data.results.length; i++) {
         if (data.results[i][0].indexOf("0000000000000000") !== -1) {
           data.results[i][0] = data.results[i][0].split("0000000000000000")[1];
           hadToSplit = true;
         }
         data.results[i][1] = parseInt(data.results[i][1]);
      }

      chart = new Highcharts.Chart({
            chart: {
                renderTo: id,
                plotBackgroundColor: null,
                plotBorderWidth: null,
                plotShadow: false,
                backgroundColor: "#EEE"
            },
            title: {
                text: data.columnNames[0] + " " + data.columnNames[1]
            },
            tooltip: {
                formatter: function() {
                  if (Type.type === 2 && level===0) {
                    return '<b>'+ regionMap[this.point.config[0]] +'</b>: '+ this.percentage +' %';
                  }
                  if (this.point.config[0].indexOf("0000000") != -1) {
                    var parsed = parseInt(this.point.config[0]);
                    return '<b>'+ parsed +'</b>: '+ this.percentage +' %';
                  } else {
                    return '<b>'+ this.point.name +'</b>: '+ this.percentage +' %';
                  }
                }
            },
            plotOptions: {
                series: {
                            cursor: 'pointer',
                            point: {
                                events: {
                                    click: function() {
                                      if (level == 0) {
                                        var name = this.config[0];
                                        if (State.level1Left) {
                                          level1Filter.right = name;
                                          DrawLogic.drawGraphFor(1, name, "R");
                                          State.level1Right = true;
                                        } else {
                                          level1Filter.left = name;
                                          DrawLogic.drawGraphFor(1, name, "L");
                                          State.level1Left = true;
                                        }
                                      } else if(level == 1) {
                                        var name = this.config[0];
                                        if (hadToSplit) {
                                          name = "0000000000000000" + name;
                                        }
                                        if (clicker == "L") {
                                          DrawLogic.drawGraphFor(2, name, "L");

                                        } else {
                                          DrawLogic.drawGraphFor(2, name, "R");
                                        }
                                      } else {
                                        
                                      }
                                    }
                                }
                            }
                        },
                pie: {
                    allowPointSelect: true,
                    cursor: 'pointer',
                    dataLabels: {
                        enabled: true,
                        color: '#000000',
                        connectorColor: '#000000',
                        formatter: function() {
                          if (Type.type === 2 && level===0) {
                            return '<b>'+ regionMap[this.point.config[0]] +'</b>: '+ this.point.config[1];
                          } else {
                            return '<b>'+ this.point.name +'</b>: '+ this.point.config[1];
                          }
                        }
                    }
                }
            },
            series: [{
                type: 'pie',
                name: data.columnNames[0] + " " + data.columnNames[1],
                data: data.results
            }]
        });
   },

   drawSecongGraph: function(d) {
      var data = d.elements[0];
      console.log(data);
      var valArr = [];
      var timeArr = [];
      for (var i=0; i < data.results.length; i++) {
         valArr[i] = parseInt(data.results[i][1]);
      }
      chart = new Highcharts.Chart({
            chart: {
                renderTo: 'graph3',
                type: 'line',
                marginRight: 130,
                marginBottom: 25
            },
            title: {
                text: data.columnNames[0] + " : " + data.columnNames[1],
                x: -20 //center
            },
            subtitle: {
                text: '',
                x: -20
            },
            xAxis: {
                categories: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            },
            yAxis: {
                title: {
                    text: data.columnNames[1]
                },
                plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },
            tooltip: {
                formatter: function() {
                        return '<b>'+ this.series.name +'</b><br/>'+
                        this.x +': '+ this.y ;
                }
            },
            legend: {
                layout: 'vertical',
                align: 'right',
                verticalAlign: 'top',
                x: -10,
                y: 100,
                borderWidth: 0
            },
            series: [{
                name: data.columnNames[1],
                data: valArr
            }]
        });
   }

}