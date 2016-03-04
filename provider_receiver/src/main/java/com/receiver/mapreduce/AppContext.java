package com.receiver.mapreduce;

import com.receiver.dataformats.Convertor;
import com.receiver.dataformats.ConvertorImpl;
import com.receiver.facebookprovider.FacebookUserSegmentsMapper;
import com.receiver.nexusprovider.NexusUserSegmentsMapper;


/**
 * Contains all necessary classes, being a connecting link of application
 * @author Ramizjon
 *
 */
public class AppContext {

    /**
     * implementation of nexus application mapper class
     *
     * @author Ramizjon
     *
     */
    public static class NexusUserSegmentsMapperImpl extends NexusUserSegmentsMapper {
        @Override
        protected Convertor getConvertor() {
            return new ConvertorImpl();
        }
    }

    /**
     * implementation of facebook application mapper class
     *
     * @author Ramizjon
     *
     */
    public static class FacebookUserSegmentsMapperImpl extends FacebookUserSegmentsMapper {

        @Override
        protected Convertor getConvertor() {
            return new ConvertorImpl();
        }
    }


}
