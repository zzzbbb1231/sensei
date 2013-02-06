<script id="segment" type="text/x-handlebars-template">
<div class="segment-item" style="margin:20px;padding:10px">

  <div class="segment-header">
    <div class="segment-main-info">
      <div class="av-actions" style="color:#069;font-weight:bold;float:right">
        <span class="move segment-actions" style="cursor:pointer">move</span>
        <span></span>
        <span class="delete segment-actions" style="cursor:pointer">delete</span>
      </div>

      <div class="segment-name"> 
        Id : <span class="data">{{segmentId}}</span>
      </div>

      {{#if indexVersion}}
        <div class="index-version" >
          Index Version : <b class="data">{{indexVersion}}</b>
        </div>
      {{/if}}

      <div class="segment-name"> 
        Partition : <span class="data">{{partitionName}}</span>
      </div>
    </div>

    <div class="segment-info">
      {{#if startTime}}
        <div>StartTime : <span class="data">{{startTime}}</span></div>
      {{/if}}

      {{#if endTime}}
        <div>EndTime : <span class="data">{{endTime}}</span></div>
      {{/if}}

      {{#if timeType}}
        <div>TimeType : <span class="data">{{timeType}}</span></div>
      {{/if}}

      {{#if aggregation}}
        <div>Aggregation : <span class="data">{{aggregation}}</span></div>
      {{/if}}

      {{#if clusterName}}
        <div>ClusterName : <span class="data">{{clusterName}}</span></div>
      {{/if}}
    </div>

    <div class="nodes-list">
      {{#each pathUrl}}
        <div><a href="{{this}}">{{this}}</a></div>
      {{/each}}
    </div>
  </div>
</div>

<div class="cancel-confirm-overlay">
</div>

<div class="confim-dialog-box">
  <div class="confirm-message">
    Are you Sure that you want to delete <b>{{segmentId}}</b> from partition <b>{{partitionName}}</b> ?
  </div>
  <div class="confirm-actions">
    <div class="cancel-button float-left">Cancel</div>
    <div class="seperator">    </div>
    <div class="delete-button float-left">Delete</div>
  </div>
</div>

<div class="confirm-move-box">
  <div class="message info">The current partition for <b>{{segmentId}}</b> is <b>{{partitionName}}</b>.</div>  
  <div class="message take-care">Be Sure to Select a partition before moving..</div>
  <div class="partitions-select-for-move">
    <select id="partions-select-for-move-drop-down" name="select1" class="partitions-move-dropdown">
      {{#each partitions}}
        <option>{{this}}</option>
      {{/each}}
    </select>
  </div>
  <div class="confirm-actions">
    <div class="cancel-button float-left">Cancel</div>
    <div class="seperator"></div>
    <div class="move-button float-left">Move</div>
    <div class="mover-error"></div>
  </div>
</div>
</script>