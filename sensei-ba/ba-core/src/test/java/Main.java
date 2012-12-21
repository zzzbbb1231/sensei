

/*
public class Main {
private static BufferedWriter bufferedWriter;
public static void main(String[] args) throws Exception {
  File json = new File(Main.class.getClassLoader().getResource("data/accountIds.json").toURI());
  JSONObject jsonObj = new JSONObject(IOUtils.toString(new FileInputStream(json)));
  String[] states = new String[]{"AL",  "AZ", "CA", "CO", "CT",  "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"};
  final List<JSONObject> ret = new ArrayList<JSONObject>();
  int count = 0;
  for (String state : states) {
    String jsonForState = FileUploadUtils.getStringResponse("http://data.cnn.com/jsonp/ELECTION/2012/" + state + "/xpoll/Pfull.json");
    Map<String, Object> newContext = new HashMap<String, Object>();
    newContext.put("state", state);
    try {
    ret.addAll(traverseJson(null, new JSONObject(jsonForState.substring(jsonForState.indexOf("(") + 1, jsonForState.indexOf(")"))), newContext));
    count++;
    } catch (Exception ex) {
      System.out.println(ex.getMessage() + "; " + "http://data.cnn.com/jsonp/ELECTION/2012/" + state + "/xpoll/Pfull.json");
    }
  }
  final int[] permutationArray = new int[ret.size()];
  for (int i = 0; i < permutationArray.length; i++) {
    permutationArray[i] = i;
  }
  final String[] sortedColumns = new String[] {"electiondate", "state", "race", "pollname", "question", "answer", "fname"}; 
  SortUtil.quickSort(0, permutationArray.length, new SortUtil.IntComparator() {
    @Override
    public int compare(Integer o1, Integer o2) {
      return compare(o1.intValue(), o2.intValue());
    }
    @Override
    public int compare(int k1, int k2) {
      for (String column :  sortedColumns) {
        String value1;
        try {
          value1 = ret.get(k1).getString(column);
       
        String value2 = ret.get(k2).getString(column);
        int cmp = value1.compareTo(value2);
        if (cmp != 0) {
          return cmp;
        }
        } catch (JSONException e) {
          throw new RuntimeException(e);
        }
      }
      return 0; 
    }
  }, new SortUtil.Swapper() {
    @Override
    public void swap(int a, int b) {
      int tmp = permutationArray[b];
      permutationArray[b] = permutationArray[a];
      permutationArray[a] = tmp;
    }
  });
  bufferedWriter = new BufferedWriter(new FileWriter("USElectionPolls2012.json"));
  for (int i = 0; i < permutationArray.length; i++) {
    bufferedWriter.write(ret.get(permutationArray[i]) + "\n");
  }
  bufferedWriter.close();
  List<String> accountIds = new ArrayList<String>(2000);
  JSONArray campaignFacets = jsonObj.getJSONObject("facets").getJSONArray("campaignId");
  for (int i = 0; i < campaignFacets.length(); i++) {
    accountIds.add("" + Integer.parseInt(campaignFacets.getJSONObject(i).getString("value")));
  }
  SenseiClientRequest senseiRequest = SenseiClientRequest.builder().addSelection(Selection.terms("campaignId", accountIds.subList(0, 500), Collections.EMPTY_LIST, Operator.or)).build();
  JSONObject serialized = (JSONObject) JsonSerializer.serialize(senseiRequest);
  System.out.println(serialized.toString(1));
  String senseiResult = new SenseiServiceProxy("localhost", 8080).sendPostRaw("http://localhost:8080/sensei", serialized.toString());
  System.out.println(senseiResult);

}
    public static List<JSONObject> traverseJson(String parentKey, JSONObject object, Map<String, Object> context) throws Exception {
      List<JSONObject> ret = new ArrayList<JSONObject>();
      Iterator<String> keys = object.keys();
      Map<String, Object> newContext = new HashMap<String, Object>(context);
      Map<String, Object> childs = new HashMap<String, Object>();
      while(keys.hasNext()) {
        String key = keys.next();
        Object value = object.get(key);
        if (value instanceof JSONObject || value instanceof JSONArray) {
          childs.put(key, value);
        } else {
          newContext.put(key, value);
        }
      }
      for (String key : childs.keySet()) {
        Object child = childs.get(key);
        if (child instanceof JSONObject) {
          ret.addAll(traverseJson(key, (JSONObject) child, newContext));
        } else if (child instanceof JSONArray) {
          JSONArray array = (JSONArray) child;
          for (int i = 0; i < array.length(); i++) {
            JSONObject element = array.getJSONObject(i);
            ret.addAll(traverseJson(key, (JSONObject) element, newContext));
          }
        }
      }
      if (childs.size() == 0 && "candidateanswers".equals(parentKey)) {
        newContext.remove("title");
        newContext.remove("title2");
        newContext.remove("totalPages");
        //newContext.remove("rid");
        int id = (Integer) newContext.remove("id");
        if (id == 0) {
          newContext.put("party", "Other/No Answer");
          newContext.put("fname", "Other/No Answer");
          newContext.put("lname", "Other/No Answer");
        } else if (id == 893) {
          newContext.put("party", "Republicans");
          newContext.put("fname", "Mitt");
          newContext.put("lname", "Romney");
        } else if (id == 1918) {
          newContext.put("party", "Democrats");
          newContext.put("fname", "Barack");
          newContext.put("lname", "Obama");
        } else {
          throw new UnsupportedOperationException("" + id);
        }
        Integer numrespondents = getIntParam(newContext, "numrespondents");
        
        Integer pct = getIntParam(newContext, "pct");
        if (numrespondents != null && pct != null) {
          newContext.put("numSupported", "" + (numrespondents * pct / 100));
        } else {
          newContext.put("numSupported", "N/A");
        }
        JSONObject newObj = new JSONObject(newContext);
        ret.add(newObj);
      }
      return ret;
    }
    public static Integer getIntParam(Map<String, Object> newContext, String string) {
      String str = newContext.get(string).toString();
      if (StringUtils.isNumeric(str)) {
        return Integer.parseInt(str);
      }
     return null;
    }

}
*/