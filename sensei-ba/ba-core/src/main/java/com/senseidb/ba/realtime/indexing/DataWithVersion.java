package com.senseidb.ba.realtime.indexing;


public interface DataWithVersion {
      public Object[] getValues();
      public String getVersion();
      
      public static class DataWithVersionImpl implements DataWithVersion {
        Object[] values;
        String version;
        public DataWithVersionImpl(Object[] values, long version) {
          super();
          this.values = values;
          this.version = String.valueOf(version);
        }
        @Override
        public Object[] getValues() {
          return values;
        }
        @Override
        public String getVersion() {
          return version;
        }
        
      }
}
