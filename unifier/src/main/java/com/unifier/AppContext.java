package com.unifier;

import com.unifier.facebookprovider.FacebookConvertor;
import com.unifier.facebookprovider.FacebookUserSegmentsMapper;
import com.unifier.nexusprovider.NexusConvertor;
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
        protected NexusConvertor getConvertor() {
            return new NexusConvertor();
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
        protected FacebookConvertor getConvertor() {
            return new FacebookConvertor();
        }
    }


}
