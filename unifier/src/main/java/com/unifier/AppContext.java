package com.unifier;

import com.common.mapreduce.ReducerUserModCommand;
import com.unifier.facebookprovider.FacebookConvertor;
import com.unifier.nexusprovider.NexusConvertor;
import utils.Convertor;

import java.util.AbstractMap;
import java.util.List;


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
    public static class NexusUserSegmentsMapperImpl extends UnifierUserSegmentsMapper {

        @Override
        protected Convertor<AbstractMap.SimpleEntry<String,Context>, List<ReducerUserModCommand>> getConvertor() {
            return new NexusConvertor();
        }

        @Override
        protected String getProviderTypeName() {
            return "nexus";
        }
    }

    /**
     * implementation of facebook application mapper class
     *
     * @author Ramizjon
     *
     */
    public static class FacebookUserSegmentsMapperImpl extends UnifierUserSegmentsMapper {

        @Override
        protected FacebookConvertor getConvertor() {
            return new FacebookConvertor();
        }

        @Override
        protected String getProviderTypeName() {
            return "facebook";
        }
    }


}
