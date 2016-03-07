package com.unifier.mapreduce;

import com.unifier.dataformats.Convertor;
import com.unifier.dataformats.ConvertorImpl;
import com.unifier.facebookprovider.FacebookUserSegmentsMapper;
import com.unifier.nexusprovider.NexusUserSegmentsMapper;


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
