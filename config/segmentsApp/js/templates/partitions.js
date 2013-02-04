<script id="partitions" type="text/x-handlebars-template">
  <select id="partions-select-drop-down" name="select1" class="partitions-dropdown" style="width:100%">
    <option>Partition-all</option>
    {{#each partitions}}
      <option>Partition-{{this}}</option>
      {{/each}}
  </select>

  <div class="predefined-filters">
  <div class="sub-heading">Filter Options:</div>
    <div class="predefined">
      <div><a href="#" id="predefined-action-today" class="predefined-actions">Today</a></div>
      <div><a href="#" id="predefined-action-yesterday" class="predefined-actions">Yesterday</a></div>
      <div><a href="#" id="predefined-action-this-week" class="predefined-actions">This Week</a></div>
      <div><a href="#" id="predefined-action-this-month" class="predefined-actions">This Month</a></div>
    </div>
  </div>

  <br>
  <div class="custom-filters">
    <div class="sub-heading">Custom:</div>
    <div class="from-selection">
      From : <input type="text" id="custom-time-from" class="partion-view-input"/>
    </div>
    <div class="to-selection">
      To : <input type="text" id="custom-time-to" class="partion-view-input" />
    </div>
  </div>

  <div class="filter-button">Filter</div>
</script>