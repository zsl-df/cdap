/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.lang;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


/**
 * ClassLoader that filters out certain resources.
 */
public final class FilterClassLoader extends ClassLoader {
  //private static final Logger LOG = LoggerFactory.getLogger(FilterClassLoader.class);


  private final ClassLoader extensionClassLoader;
  private final Filter filter;

  /**
   * Represents filtering  that the {@link FilterClassLoader} needs to apply.
   */
  public interface Filter {

    /**
     * Returns the result of whether the given resource is accepted or not.
     */
    boolean acceptResource(String resource);

    /**
     * Returns the result of whether the given package is accepted or not.
     */
    boolean acceptPackage(String packageName);
  }

  /**
   * Returns the default filter that should applies to all program type. By default
   * all hadoop classes and cdap-api classes (and dependencies) are allowed.
   */
  public static Filter defaultFilter() {
    final Set<String> visibleResources = ProgramResources.getVisibleResources();
    final Set<String> visiblePackages = new HashSet<>();
    for (String resource : visibleResources) {
      if (resource.endsWith(".class")) {
        int idx = resource.lastIndexOf('/');
        // Ignore empty package
        if (idx > 0) {
          visiblePackages.add(resource.substring(0, idx).replace('/', '.'));
        }
      }
    }

       //visiblePackages.add("com.google.protobuf");

//       System.out.println("sbbbbbbbbbb visiblePackages::" + visiblePackages.toString());
//       System.out.println("sbbbbbbbbbb visibleResources::" + visibleResources.toString());
	
    return new Filter() {
      @Override
      public boolean acceptResource(String resource) {
//      	if(resource.startsWith("org.apache.arrow.vector")) {
//      		
//      		System.out.println("FilterClassLoader.acceptResource ");
//    		for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
//    		    System.out.println("\t " + ste);
//    		}
//    	}
        return visibleResources.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
//      	if(packageName.startsWith("org.apache.arrow.vector")) {
//      		
//      		System.out.println("Filter.acceptPackage");
//    		for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
//    		    System.out.println("\t" + ste);
//    		}
//    	}
        return visiblePackages.contains(packageName);
      }
    };
  }

  /**
   * Creates a new {@link FilterClassLoader} that filter classes based on the {@link #defaultFilter()} on the
   * given parent ClassLoader
   *
   * @param parentClassLoader the ClassLoader to filter from.
   * @return a new intance of {@link FilterClassLoader}.
   */
  public static FilterClassLoader create(ClassLoader parentClassLoader) {
    return new FilterClassLoader(parentClassLoader, defaultFilter());
  }

  /**
   * Create a {@link FilterClassLoader} that filter classes based on the given {@link Filter} on the given
   * parent ClassLoader.
   *
   * @param parentClassLoader Parent ClassLoader
   * @param filter Filter to apply for the ClassLoader
   */
  public FilterClassLoader(ClassLoader parentClassLoader, Filter filter) {
    super(parentClassLoader);
    this.extensionClassLoader = new URLClassLoader(new URL[0], ClassLoader.getSystemClassLoader().getParent());
    this.filter = filter;
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    // Try to load it from bootstrap class loader first
	  
//	  if(name.startsWith("com.google.protobuf") || name.startsWith("org.apache.arrow.vector")) {
//		  System.out.println(" FilterClassLoader trying to load class :: " + name);
//		  System.out.println(" filter.getClass().getName():: " + filter.getClass().getName());
//	  }
	  
    try {
      return extensionClassLoader.loadClass(name);
    } catch (ClassNotFoundException e) {
    	
//    	if(name.startsWith("com.google.protobuf") || name.startsWith("org.apache.arrow.vector")) {
//  		  System.out.println(" FilterClassLoader.loadClass ClassNotFoundException trying to load class :: " + name
//  				  + ", filter.getClass() :: " + filter.getClass()
//  				  + ", classNameToResourceName(name):: " + classNameToResourceName(name)
//  				  + " , accepted? :: " + filter.acceptResource(classNameToResourceName(name)));
//  	  	}
//    	if(name.startsWith("org.apache.arrow.vector") && filter.getClass().getName().contains("SparkResourceFilters")) {
//    		try {
//    	    	//Class c = Class.forName("co.cask.cdap.app.runtime.spark.SparkResourceFilters");
//    			//Class c = Class.forName(args[0]);
//    			System.out.println("class SparkResourceFilters::::::::::" + filter.getClass().getProtectionDomain().getCodeSource().getLocation());
//    		}catch(Exception ee) {
//    			ee.printStackTrace();
//    			
//    		}
//    	}

    	
      if (filter.acceptResource(classNameToResourceName(name))) {
        return super.loadClass(name, resolve);
      }
      throw e;
    }
  }

  @Override
  protected Package[] getPackages() {
    List<Package> packages = Lists.newArrayList();
    for (Package pkg : super.getPackages()) {
      if (filter.acceptPackage(pkg.getName())) {
        packages.add(pkg);
      }
    }
    return packages.toArray(new Package[packages.size()]);
  }

  @Override
  protected Package getPackage(String name) {
    // Replace all '/' with '.' since Java allow both names like "java/lang" or "java.lang" as the name to lookup
    return (filter.acceptPackage(name.replace('/', '.'))) ? super.getPackage(name) : null;
  }

  @Override
  public URL getResource(String name) {
	  
//	  if(name.startsWith("com.google.protobuf") || name.startsWith("org.apache.arrow.vector")) {
//		  System.out.println("FilterClassLoader trying to load resource getResource:: " + name);
//	  }
	  
    URL resource = extensionClassLoader.getResource(name);
    if (resource != null) {
      return resource;
    }
    return filter.acceptResource(name) ? super.getResource(name) : null;
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
	  
//	  if(name.startsWith("com.google.protobuf") || name.startsWith("org.apache.arrow.vector")) {
//		  System.out.println("FilterClassLoader trying to load resource getResources:: " + name);
//	  }
	  
    Enumeration<URL> resources = extensionClassLoader.getResources(name);
    if (resources.hasMoreElements()) {
      return resources;
    }
    return filter.acceptResource(name) ? super.getResources(name) : Collections.<URL>emptyEnumeration();
  }

  @Override
  public InputStream getResourceAsStream(String name) {
	  
//	  if(name.startsWith("com.google.protobuf") || name.startsWith("org.apache.arrow.vector")) {
//		  System.out.println("trying to load getResourceAsStream :: " + name);
//	  }
	  
    InputStream resourceStream = extensionClassLoader.getResourceAsStream(name);
    if (resourceStream != null) {
      return resourceStream;
    }
    return filter.acceptResource(name) ? super.getResourceAsStream(name) : null;
  }

  private String classNameToResourceName(String className) {
    return className.replace('.', '/') + ".class";
  }
}
