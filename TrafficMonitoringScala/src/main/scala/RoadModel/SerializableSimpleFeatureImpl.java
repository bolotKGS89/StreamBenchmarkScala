package RoadModel;

import java.io.Serializable;
import java.util.List;

import org.geotools.feature.simple.SimpleFeatureImpl;

import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.identity.FeatureId;

public class SerializableSimpleFeatureImpl extends SimpleFeatureImpl implements Serializable {

    private static final long serialVersionUID = 1L;

    public SerializableSimpleFeatureImpl(List<Object> values, SimpleFeatureType featureType,
                                         FeatureId id) {
        super(values, featureType, id);
    }

}