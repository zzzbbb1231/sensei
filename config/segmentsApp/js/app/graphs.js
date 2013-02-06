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

	initGraph: function(d) {
      var data = d.elements[0];
      console.log(data);
		chart1 = new Highcharts.Chart({
         chart: {
            renderTo: 'graph1',
            type: 'bar'
         },
         height: 100,
         width: 100,
         title: {
            text: data.columnNames[0]
         },
         xAxis: {
            categories: data.columnNames
         },
         yAxis: {
            title: {
               text: data.columnNames[0]
            }
         },
         series: [{
            name: '',
            data: [parseInt(data.results[0][0])]
         }]
      });
	},

   drawFirstGraph: function(d) {
      var data = d.elements[0];
      for (var i=0; i < data.results.length; i++) {
         data.results[i][1] = parseInt(data.results[i][1]);
      }
      chart = new Highcharts.Chart({
            chart: {
                renderTo: 'graph2',
                plotBackgroundColor: null,
                plotBorderWidth: null,
                plotShadow: false
            },
            title: {
                text: data.columnNames[0] + " " + data.columnNames[1]
            },
            tooltip: {
                formatter: function() {
                    return '<b>'+ this.point.name +'</b>: '+ this.percentage +' %';
                }
            },
            plotOptions: {
                pie: {
                    allowPointSelect: true,
                    cursor: 'pointer',
                    dataLabels: {
                        enabled: true,
                        color: '#000000',
                        connectorColor: '#000000',
                        formatter: function() {
                            return '<b>'+ this.point.name +'</b>: '+ this.point.config[1] ;
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